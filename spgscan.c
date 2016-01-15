#include "postgres.h"

#include "access/relscan.h"
#include "pgstat.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "spgist.h"

PG_FUNCTION_INFO_V1(spgbeginscan);
Datum       spgbeginscan(PG_FUNCTION_ARGS);
Datum
spgbeginscan(PG_FUNCTION_ARGS)
{
    Relation    rel = (Relation) PG_GETARG_POINTER(0);
	int         keysz = PG_GETARG_INT32(1);
	/* ScanKey     scankey = (ScanKey) PG_GETARG_POINTER(2); */
	IndexScanDesc scan;
	SpGistScanOpaque so;

	scan = RelationGetIndexScan(rel, keysz, 0);

	so = (SpGistScanOpaque) palloc(sizeof(SpGistScanOpaqueData));
	initSpGistState(&so->state, scan->indexRelation);
	so->tempCxt = AllocSetContextCreate(CurrentMemoryContext,
										"SpGist search temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	scan->opaque = so;

	PG_RETURN_POINTER(scan);
}

PG_FUNCTION_INFO_V1(spgrescan);
Datum       spgrescan(PG_FUNCTION_ARGS);
Datum
spgrescan(PG_FUNCTION_ARGS)
{
    IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	/* SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque; */
	ScanKey     scankey = (ScanKey) PG_GETARG_POINTER(1);

	if (scankey && scan->numberOfKeys > 0)
	{
		if (scan->numberOfKeys != 1)
			elog(ERROR, "SPGIST doesn;t support multiple operation (yet)");

		memmove(scan->keyData, scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spgendscan);
Datum       spgendscan(PG_FUNCTION_ARGS);
Datum
spgendscan(PG_FUNCTION_ARGS)
{
	IndexScanDesc scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	SpGistScanOpaque so = (SpGistScanOpaque) scan->opaque;

	MemoryContextDelete(so->tempCxt);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spgmarkpos);
Datum       spgmarkpos(PG_FUNCTION_ARGS);
Datum
spgmarkpos(PG_FUNCTION_ARGS)
{
    elog(ERROR, "SpGist does not support mark/restore");
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spgrestrpos);
Datum       spgrestrpos(PG_FUNCTION_ARGS);
Datum
spgrestrpos(PG_FUNCTION_ARGS)
{
	elog(ERROR, "SpGist does not support mark/restore");
	PG_RETURN_VOID();
}

static bool
spgLeafTest(SpGistState *state, MemoryContext ctx, SpGistLeafTuple tuple, int level, TIDBitmap *tbm, Datum datum)
{
	bool 			result;
	MemoryContext	oldCtx;
	
	oldCtx = MemoryContextSwitchTo(ctx);
	result = DatumGetBool(FunctionCall3(&state->leafConsistentFn,
										Int32GetDatum(level),
										datum,
										SGLTDATUM(tuple, state)));
	MemoryContextSwitchTo(oldCtx);

	if (result)
	{
#if 0
		elog(NOTICE, "AA %u:%u", 
						ItemPointerGetBlockNumber(&tuple->heapPtr),
						ItemPointerGetOffsetNumber(&tuple->heapPtr));
#endif
		tbm_add_tuples(tbm, &tuple->heapPtr, 1, false);
	}

	return result;
}


static void
spgsearch(Relation index, SpGistScanOpaque so, TIDBitmap *tbm, int64 *ntids, 
			Datum datum, int level, BlockNumber blkno, OffsetNumber offset)
{
	Buffer			buffer;
	Page			page;

	buffer = ReadBuffer(index, blkno);
	LockBuffer(buffer, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buffer);

	if (SpGistPageIsLeaf(page))
	{
		SpGistLeafTuple 	leafTuple;

		if (offset == InvalidOffsetNumber)
		{
			for(offset=FirstOffsetNumber; offset<=SpGistPageGetMaxOffset(page); offset ++)
			{
				leafTuple = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, offset));

				(*ntids) += !!spgLeafTest(&so->state, so->tempCxt, leafTuple, level, tbm, datum);
			}
		}
		else
		{
			while(offset != InvalidOffsetNumber)
			{
				leafTuple = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, offset));

				(*ntids) += !!spgLeafTest(&so->state, so->tempCxt, leafTuple, level, tbm, datum);

				offset = leafTuple->nextOffset; 
			}
		}

		MemoryContextReset(so->tempCxt);
	} 
	else 
	{
		SpGistInnerTuple		innerTuple;
		spgInnerConsistentIn	in;
		spgInnerConsistentOut	out;
		IndexTuple				*nodes, node;
		int						i, *nodeNumbers, nNodes;
		MemoryContext			oldCtx;

		if (offset == InvalidOffsetNumber)
			offset = FirstOffsetNumber;
		i = SpGistPageGetMaxOffset(page);
		Assert( offset <= SpGistPageGetMaxOffset(page));

		innerTuple = (SpGistInnerTuple) PageGetItem(page, PageGetItemId(page, offset));

		in.query = datum;
		in.level = level;
		in.hasPrefix = innerTuple->hasPrefix;
		in.prefixDatum = SGITDATUM(innerTuple, &so->state);
		in.nNodes = innerTuple->nNodes;
		nodes = palloc(sizeof(IndexTuple) * in.nNodes); 

		oldCtx = MemoryContextSwitchTo(so->tempCxt);
		in.nodeDatums = palloc(sizeof(Datum) * in.nNodes);

		SGITITERATE(innerTuple, &so->state, i, node)
		{
			bool	isnull;

			nodes[i] = node;
			in.nodeDatums[i] = index_getattr(node, 1, so->state.nodeTupDesc, &isnull);
		}
		
		FunctionCall2(&so->state.innerConsistentFn,
						PointerGetDatum(&in),
						PointerGetDatum(&out));
		MemoryContextSwitchTo(oldCtx);

		nNodes = out.nNodes;

		if (nNodes > 0)
		{
			nodeNumbers = palloc(sizeof(int) * nNodes);
			memcpy(nodeNumbers, out.nodeNumbers, nNodes * sizeof(int));
		}

		MemoryContextReset(so->tempCxt);

		level += out.levelAdd;

		for(i=0; i<nNodes; i++)
		{
			spgsearch(index, so, tbm, ntids, datum, level, 
						ItemPointerGetBlockNumber(&nodes[nodeNumbers[i]]->t_tid),
						ItemPointerGetOffsetNumber(&nodes[nodeNumbers[i]]->t_tid));
		}

		if (nNodes > 0)
			pfree(nodeNumbers);
		pfree(nodes);
	}

	UnlockReleaseBuffer(buffer);
}


PG_FUNCTION_INFO_V1(spggetbitmap);
Datum       spggetbitmap(PG_FUNCTION_ARGS);
Datum
spggetbitmap(PG_FUNCTION_ARGS)
{
	IndexScanDesc 			scan = (IndexScanDesc) PG_GETARG_POINTER(0);
	TIDBitmap  				*tbm = (TIDBitmap *) PG_GETARG_POINTER(1);
	int64					ntids = 0;
	SpGistScanOpaque 		so = (SpGistScanOpaque) scan->opaque;

	spgsearch(scan->indexRelation, so, tbm, &ntids, 
			scan->keyData->sk_argument, 0, SPGIST_HEAD_BLKNO, InvalidOffsetNumber);
	
	PG_RETURN_INT64(ntids);
}

