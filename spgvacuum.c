#include "postgres.h"

#include "access/genam.h"
#include "catalog/storage.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "postmaster/autovacuum.h"
#include "storage/bufmgr.h"
#include "storage/indexfsm.h"
#include "storage/lmgr.h"

#include "spgist.h"

PG_FUNCTION_INFO_V1(spgbulkdelete);
Datum       spgbulkdelete(PG_FUNCTION_ARGS);
Datum
spgbulkdelete(PG_FUNCTION_ARGS)
{
	IndexVacuumInfo 		*info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult 	*stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation    			index = info->index;
	BlockNumber             blkno,
							npages;
	FreeBlockNumberArray	notFullPage;
	int						countPage = 0;
	SpGistState				state;
	bool					needLock;
	Buffer					buffer;
    Page            		page;


	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	initSpGistState(&state, index); 

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	for(blkno=SPGIST_HEAD_BLKNO; blkno<npages; blkno++)
	{
		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);

        LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buffer);

#if 0
	IndexBulkDeleteCallback callback = (IndexBulkDeleteCallback) PG_GETARG_POINTER(2);
	void       				*callback_state = (void *) PG_GETARG_POINTER(3);
		if (!SpGistPageIsDeleted(page))
		{
        	SpGistTuple	*itup = SpGistPageGetData(page);
			SpGistTuple	*itupEnd = (SpGistTuple*)( ((char*)itup) + 
								 state.sizeOfSpGistTuple * SpGistPageGetMaxOffset(page));
			SpGistTuple	*itupPtr = itup;

			while(itup < itupEnd)
			{
				if (callback(&itup->heapPtr, callback_state))
				{
					stats->tuples_removed += 1;
					START_CRIT_SECTION();
					SpGistPageGetOpaque(page)->maxoff--;
					END_CRIT_SECTION();
				} 
				else 
				{
					if ( itupPtr != itup )
					{
						START_CRIT_SECTION();
						memcpy(itupPtr, itup, state.sizeOfSpGistTuple);
						END_CRIT_SECTION();
					}
					stats->num_index_tuples++;
					itupPtr = (SpGistTuple*)( ((char*)itupPtr) + state.sizeOfSpGistTuple );
				}

				itup = (SpGistTuple*)( ((char*)itup) + state.sizeOfSpGistTuple );
			}

			if (itupPtr != itup)
			{
				if (itupPtr == SpGistPageGetData(page))
				{
					START_CRIT_SECTION();
					SpGistPageSetDeleted(page);
					END_CRIT_SECTION();
				}
				MarkBufferDirty(buffer);
			}

			if (!SpGistPageIsDeleted(page) && 
						SpGistPageGetFreeSpace(&state, page) > state.sizeOfSpGistTuple && 
						countPage < SpGistMetaBlockN)
				notFullPage[countPage++] = blkno;
		}
#endif

        UnlockReleaseBuffer(buffer);
		CHECK_FOR_INTERRUPTS();
	}

	if (countPage>0) 
	{
		SpGistMetaPageData	*metaData;

		buffer = ReadBuffer(index, SPGIST_METAPAGE_BLKNO);
		LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
		page = BufferGetPage(buffer);

		metaData = SpGistPageGetMeta(page);
		START_CRIT_SECTION();
		memcpy(metaData->notFullPage, notFullPage, sizeof(FreeBlockNumberArray));
		END_CRIT_SECTION();

		MarkBufferDirty(buffer);
        UnlockReleaseBuffer(buffer);
	}

	PG_RETURN_POINTER(stats);
}

PG_FUNCTION_INFO_V1(spgvacuumcleanup);
Datum       spgvacuumcleanup(PG_FUNCTION_ARGS);
Datum
spgvacuumcleanup(PG_FUNCTION_ARGS)
{
    IndexVacuumInfo *info = (IndexVacuumInfo *) PG_GETARG_POINTER(0);
	IndexBulkDeleteResult *stats = (IndexBulkDeleteResult *) PG_GETARG_POINTER(1);
	Relation    index = info->index;
	bool        needLock;
	BlockNumber npages,
				blkno;
	BlockNumber totFreePages;
	BlockNumber lastBlock = SPGIST_HEAD_BLKNO,
				lastFilledBlock = SPGIST_HEAD_BLKNO;

	if (info->analyze_only)
		PG_RETURN_POINTER(stats);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	needLock = !RELATION_IS_LOCAL(index);

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	npages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	totFreePages = 0;
	for (blkno = SPGIST_HEAD_BLKNO; blkno < npages; blkno++)
	{
		Buffer      buffer;
		Page        page;

		vacuum_delay_point();

		buffer = ReadBufferExtended(index, MAIN_FORKNUM, blkno,
									RBM_NORMAL, info->strategy);
		LockBuffer(buffer, BUFFER_LOCK_SHARE);
		page = (Page) BufferGetPage(buffer);
																						
		if (SpGistPageIsDeleted(page))
		{
			RecordFreeIndexPage(index, blkno);
			totFreePages++;
		}
		else
		{
			lastFilledBlock = blkno;
			stats->num_index_tuples += SpGistPageGetMaxOffset(page);
			stats->estimated_count += SpGistPageGetMaxOffset(page);
		}

		UnlockReleaseBuffer(buffer);
	}

	lastBlock = npages - 1;
	if (lastBlock > lastFilledBlock)
	{
		RelationTruncate(index, lastFilledBlock + 1);
		stats->pages_removed = lastBlock - lastFilledBlock;
		totFreePages = totFreePages - stats->pages_removed;
	}

	IndexFreeSpaceMapVacuum(info->index);
	stats->pages_free = totFreePages;

	if (needLock)
		LockRelationForExtension(index, ExclusiveLock);
	stats->num_pages = RelationGetNumberOfBlocks(index);
	if (needLock)
		UnlockRelationForExtension(index, ExclusiveLock);

	PG_RETURN_POINTER(stats);
}
