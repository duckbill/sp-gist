#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"

#include "spgist.h"

typedef struct
{
	SpGistState      spgstate;
	MemoryContext   tmpCtx;
} SpGistBuildState;

PG_FUNCTION_INFO_V1(spgbuildempty);
Datum       spgbuildempty(PG_FUNCTION_ARGS);
Datum
spgbuildempty(PG_FUNCTION_ARGS)
{
	elog(ERROR, "unsupported");
	PG_RETURN_VOID();
}

static void
spgistBuildCallback(Relation index, HeapTuple htup, Datum *values,
                    bool *isnull, bool tupleIsAlive, void *state)
{
	SpGistBuildState *buildstate = (SpGistBuildState*)state;

	if (*isnull == false)
	{
		MemoryContext oldCtx = MemoryContextSwitchTo(buildstate->tmpCtx);

		spgdoinsert(index, &buildstate->spgstate, &htup->t_self, *values);

		MemoryContextSwitchTo(oldCtx);
		MemoryContextReset(buildstate->tmpCtx);
	}
}

PG_FUNCTION_INFO_V1(spgbuild);
Datum       spgbuild(PG_FUNCTION_ARGS);
Datum
spgbuild(PG_FUNCTION_ARGS)
{
	Relation    heap = (Relation) PG_GETARG_POINTER(0);
	Relation    index = (Relation) PG_GETARG_POINTER(1);
	IndexInfo  *indexInfo = (IndexInfo *) PG_GETARG_POINTER(2);
	IndexBuildResult *result;
	double      reltuples;
	SpGistBuildState buildstate;
	Buffer      MetaBuffer, buffer;

	if (RelationGetNumberOfBlocks(index) != 0)
		elog(ERROR, "index \"%s\" already contains data",
					RelationGetRelationName(index));

	/* initialize the meta page */
	MetaBuffer = SpGistNewBuffer(index);
	buffer = SpGistNewBuffer(index);

	START_CRIT_SECTION();
	SpGistInitMetabuffer(MetaBuffer, index);
	MarkBufferDirty(MetaBuffer);
	SpGistInitBuffer(buffer, SPGIST_LEAF);
	MarkBufferDirty(buffer);
	END_CRIT_SECTION();
	UnlockReleaseBuffer(MetaBuffer);
	UnlockReleaseBuffer(buffer);

	initSpGistState(&buildstate.spgstate, index);

	buildstate.tmpCtx = AllocSetContextCreate(CurrentMemoryContext,
											"SpGist build temporary context",
											ALLOCSET_DEFAULT_MINSIZE,
											ALLOCSET_DEFAULT_INITSIZE,
											ALLOCSET_DEFAULT_MAXSIZE);


	reltuples = IndexBuildHeapScan(heap, index, indexInfo, true,
									spgistBuildCallback, (void *) &buildstate);
	MemoryContextDelete(buildstate.tmpCtx);

	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));
	result->heap_tuples = result->index_tuples = reltuples;

	PG_RETURN_POINTER(result);
}

PG_FUNCTION_INFO_V1(spginsert);
Datum       spginsert(PG_FUNCTION_ARGS);
Datum
spginsert(PG_FUNCTION_ARGS)
{
    Relation    index = (Relation) PG_GETARG_POINTER(0);
	Datum      *values = (Datum *) PG_GETARG_POINTER(1);
	bool       *isnull = (bool *) PG_GETARG_POINTER(2);
	ItemPointer ht_ctid = (ItemPointer) PG_GETARG_POINTER(3);
#ifdef NOT_USED
	Relation    heapRel = (Relation) PG_GETARG_POINTER(4);
	IndexUniqueCheck checkUnique = (IndexUniqueCheck) PG_GETARG_INT32(5);
#endif
	SpGistState       spgstate;
	MemoryContext   oldCtx;
	MemoryContext   insertCtx;
	SpGistMetaPageData   *metaData;
	Buffer              metaBuffer;

	insertCtx = AllocSetContextCreate(CurrentMemoryContext,
										"SpGist insert temporary context",
										ALLOCSET_DEFAULT_MINSIZE,
										ALLOCSET_DEFAULT_INITSIZE,
										ALLOCSET_DEFAULT_MAXSIZE);
	oldCtx = MemoryContextSwitchTo(insertCtx);

	initSpGistState(&spgstate, index);
	metaBuffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);
	LockBuffer(metaBuffer, BUFFER_LOCK_SHARE);
	metaData = SpGistPageGetMeta(BufferGetPage(metaBuffer));

	if (*isnull == false);
		spgdoinsert(index, &spgstate, ht_ctid, *values);

	UnlockReleaseBuffer(metaBuffer);
	MemoryContextSwitchTo(oldCtx);
	MemoryContextDelete(insertCtx);
	PG_RETURN_BOOL(false);
}

