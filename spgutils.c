#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "storage/lmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "access/reloptions.h"
#include "storage/freespace.h"
#include "storage/indexfsm.h"
#include "utils/lsyscache.h"

#include "spgist.h"

PG_MODULE_MAGIC;

static void
fillTypeDesc(SpGistTypeDesc *desc, Oid type)
{
	desc->type = type;

	if (type != InvalidOid)
		get_typlenbyval(type, &desc->attlen, &desc->attbyval);
}

void
initSpGistState(SpGistState *state, Relation index)
{
	RegProcedure	propOid;

	Assert(index->rd_att->natts == 1);

	propOid = index_getprocid(index, 1, SPGIST_PROP_PROC);

	state->prop = *(SpGistOpClassProp*)DatumGetPointer(OidFunctionCall0Coll(propOid, InvalidOid));

	fillTypeDesc(&state->attType, state->prop.leafType);
	fillTypeDesc(&state->attNodeType, state->prop.nodeType);
	fillTypeDesc(&state->attPrefixType, state->prop.prefixType);

	fmgr_info_copy(&(state->chooseFn),
					index_getprocinfo(index, 1, SPGIST_CHOOSE_PROC),
					CurrentMemoryContext);
	fmgr_info_copy(&(state->picksplitFn),
					index_getprocinfo(index, 1, SPGIST_PICKSPLIT_PROC),
					CurrentMemoryContext);
	fmgr_info_copy(&(state->leafConsistentFn),
					index_getprocinfo(index, 1, SPGIST_LEAFCONS_PROC),
					CurrentMemoryContext);
	fmgr_info_copy(&(state->innerConsistentFn),
					index_getprocinfo(index, 1, SPGIST_INNERCONS_PROC),
					CurrentMemoryContext);

	state->nodeTupDesc = CreateTemplateTupleDesc(1, false);
	TupleDescInitEntry(state->nodeTupDesc, (AttrNumber) 1, NULL,
						state->attNodeType.type, -1, 0);
}

/*
 * Allocate a new page (either by recycling, or by extending the index file)
 * The returned buffer is already pinned and exclusive-locked
 * Caller is responsible for initializing the page by calling SpGistInitBuffer
 */
		
Buffer
SpGistNewBuffer(Relation index)
{
	Buffer      buffer;
	bool        needLock;
				 
	/* First, try to get a page from FSM */
	for (;;)
	{
		BlockNumber blkno = GetFreeIndexPage(index);

		if (blkno == InvalidBlockNumber)
			break;

		buffer = ReadBuffer(index, blkno);
		/*
		 * We have to guard against the possibility that someone else already
		 * recycled this page; the buffer may be locked if so.
		 */
		if (ConditionalLockBuffer(buffer))
		{
			Page        page = BufferGetPage(buffer);

			if (PageIsNew(page))
				return buffer;  /* OK to use, if never initialized */

			if (SpGistPageIsDeleted(page))
				return buffer;  /* OK to use */

			LockBuffer(buffer, BUFFER_LOCK_UNLOCK);
		}

		/* Can't use it, so release buffer and try again */
		ReleaseBuffer(buffer);
	}

	/* Must extend the file */
	needLock = !RELATION_IS_LOCAL(index);
	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);

	buffer = ReadBuffer(index, P_NEW);
	LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);

	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	return buffer;
}

void
SpGistInitBuffer(Buffer b, uint16 f)
{
	SpGistInitPage(BufferGetPage(b), f, BufferGetPageSize(b));
}

void
SpGistInitPage(Page page, uint16 f, Size pageSize)
{
	SpGistPageOpaque opaque;
	PageInit(page, pageSize, sizeof(SpGistPageOpaqueData));
	opaque = SpGistPageGetOpaque(page);
	memset(opaque, 0, sizeof(SpGistPageOpaqueData));
	opaque->flags = f;
}

void
SpGistInitMetabuffer(Buffer b, Relation index)
{
	SpGistMetaPageData   *metadata;
	Page                page = BufferGetPage(b);
	SpGistInitPage(page, SPGIST_META, BufferGetPageSize(b));
	metadata = SpGistPageGetMeta(page);
	memset(metadata, 0, sizeof(SpGistMetaPageData));
	metadata->magickNumber = SPGIST_MAGICK_NUMBER;
}

PG_FUNCTION_INFO_V1(spgoptions);
Datum       spgoptions(PG_FUNCTION_ARGS);
Datum
spgoptions(PG_FUNCTION_ARGS)
{
    Datum       reloptions = PG_GETARG_DATUM(0);
	bool        validate = PG_GETARG_BOOL(1);
	bytea      *result;

	result = default_reloptions(reloptions, validate, RELOPT_KIND_GIST);

	if (result)
		PG_RETURN_BYTEA_P(result);
	PG_RETURN_NULL();
}

unsigned int
getTypeLength(SpGistTypeDesc *att, Datum datum)
{
	unsigned int	size;
	
	if (att->attbyval)
		size = sizeof(datum);
	else if (att->attlen > 0)
		size = att->attlen;
	else
		size = VARSIZE_ANY(datum);

	return MAXALIGN(size);
}

static unsigned int
memcpyDatum(void *in, SpGistTypeDesc *att, Datum datum)
{
	unsigned int 	size;
	
	if (att->attbyval)
	{
		size = sizeof(datum);
		memcpy(in, &datum, size);
	}
	else
	{
		size = (att->attlen > 0) ? att->attlen : VARSIZE_ANY(datum);
		Assert(size < 0xffff);
		memcpy(in, DatumGetPointer(datum), size);
	}

	return MAXALIGN(size);
}

SpGistLeafTuple
spgFormLeafTuple(SpGistState *state, ItemPointer heapPtr, Datum datum)
{
	SpGistLeafTuple	tup;
	unsigned int	size = SGLTHDRSZ + getTypeLength(&state->attType, datum);

	Assert(size < 0xffff);
	tup = palloc0(size);

	tup->heapPtr = *heapPtr;
	tup->nextOffset = InvalidOffsetNumber;
	tup->size = size;

	memcpyDatum(SGLTDATAPTR(tup), &state->attType, datum);

	return tup;
}

SpGistInnerTuple
spgFormInnerTuple(SpGistState *state, bool hasPrefix, Datum prefix, int nNodes, IndexTuple *nodes)
{
	SpGistInnerTuple	tup;
	unsigned int 		size;
	int 				i;
	char				*ptr;

	size = SGITHDRSZ;
	if (hasPrefix)
		size += getTypeLength(&state->attPrefixType, prefix);

	for(i=0; i<nNodes; i++)
		size += IndexTupleSize(nodes[i]);

	Assert(size < 0xffff);

	tup = palloc0(size);

	tup->hasPrefix = !!hasPrefix;
	tup->nNodes = nNodes;
	tup->size = size;

	if (tup->hasPrefix)
		memcpyDatum(SGITDATAPTR(tup), &state->attPrefixType, prefix);

	ptr= (char*)SGITNODEPTR(tup, state);

	for(i=0; i<nNodes; i++)
	{
		IndexTuple	node = nodes[i];

		memcpy(ptr, node, IndexTupleSize(node));
		ptr += IndexTupleSize(node);
	}

	return tup;
}

PG_FUNCTION_INFO_V1(spgstat);
Datum       spgstat(PG_FUNCTION_ARGS);
Datum
spgstat(PG_FUNCTION_ARGS)
{
    text    	*name=PG_GETARG_TEXT_P(0);
	char 		*relname=text_to_cstring(name);
	RangeVar   	*relvar;
	Relation    index;
	List       	*relname_list;
	Oid			relOid;
	BlockNumber	blkno = SPGIST_HEAD_BLKNO;
	BlockNumber	totalPages = 0,
				innerPages = 0,
				emptyPages = 0;
	double		usedSpace = 0.0;
	char		res[1024];
	int			bufferSize = -1;
	int64		innerTuples = 0,
				leafTuples = 0;


	relname_list = stringToQualifiedNameList(relname);
	relvar = makeRangeVarFromNameList(relname_list);
	relOid = RangeVarGetRelid(relvar, false);
	index = index_open(relOid, AccessExclusiveLock);

	if ( index->rd_am == NULL )
		elog(ERROR, "Relation %s.%s is not an index",
					get_namespace_name(RelationGetNamespace(index)),
					RelationGetRelationName(index) );
	totalPages = RelationGetNumberOfBlocks(index);

	for(blkno=SPGIST_HEAD_BLKNO; blkno<totalPages; blkno++)
	{
		Buffer	buffer;
		Page	page;

		buffer = ReadBuffer(index, blkno);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);

		page = BufferGetPage(buffer);

		if (SpGistPageIsLeaf(page))
		{
			leafTuples += SpGistPageGetMaxOffset(page);
		}
		else
		{
			innerPages++;
			innerTuples += SpGistPageGetMaxOffset(page);
		}

		if (bufferSize < 0)
			bufferSize = BufferGetPageSize(buffer) - MAXALIGN(sizeof(SpGistPageOpaqueData)) -
							SizeOfPageHeaderData;

		usedSpace += bufferSize - (PageGetFreeSpace(page) + sizeof(ItemIdData));

		if (PageGetFreeSpace(page) + sizeof(ItemIdData) == bufferSize)
			emptyPages++;

		UnlockReleaseBuffer(buffer);
	}

	index_close(index, AccessExclusiveLock);

	totalPages--; /* metapage */

	snprintf(res, sizeof(res),
		"totalPages:  %u\n"
		"innerPages:  %u\n"
		"leafPages:   %u\n"
		"emptyPages:  %u\n"
		"usedSpace:   %.2f kbytes\n"
		"freeSpace:   %.2f kbytes\n"
		"fillRatio:   %.2f%c\n"
		"leafTuples:  %lld\n"
		"innerTuples: %lld",
			totalPages, innerPages, totalPages - innerPages, emptyPages,
			usedSpace / 1024.0,
			(( (double) bufferSize ) * ( (double) totalPages ) - usedSpace) / 1024,
			100.0 * ( usedSpace / (( (double) bufferSize ) * ( (double) totalPages )) ),
			'%',
			leafTuples, innerTuples
	);

	PG_RETURN_TEXT_P(CStringGetTextDatum(res));
}

