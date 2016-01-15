#include "postgres.h"

#include "access/genam.h"
#include "catalog/index.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/itemptr.h"
#include "storage/indexfsm.h"
#include "utils/memutils.h"

#include "spgist.h"

static void
updateNodeLink(SpGistState *state, SpGistInnerTuple tup, int nodeN, 
				BlockNumber blkno, OffsetNumber offset)
{
	int 		i;
	IndexTuple	node;

	SGITITERATE(tup, state, i, node)
	{
		if (i == nodeN)
		{
			ItemPointerSet(&node->t_tid, blkno, offset); 
			return;
		}
	}

	abort();
}

static int
cmpOffsetNumbers(const void *a, const void *b)
{
	if ( *(OffsetNumber*)a == *(OffsetNumber*)b )
		return 0;
	return ( *(OffsetNumber*)a > *(OffsetNumber*)b ) ? 1 : -1;
}

static void
doPickSplit(Relation index, SpGistState *state, Buffer buffer,  
			Buffer parentBuffer, BlockNumber *blkno /* out */, OffsetNumber *offset /* in/out */)
{
	spgPickSplitIn		in;
	spgPickSplitOut 	out;
	int					i = *offset, n = 0;
	Page				page = BufferGetPage(buffer);
	SpGistInnerTuple	innerTuple;
	IndexTuple			*nodes;
	Buffer				*leafBuffers;
	ItemPointerData		*heapPtrs;

	while( i != InvalidOffsetNumber )
	{
		SpGistLeafTuple	it = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, i));

		n++;
		i = it->nextOffset;
	}

	heapPtrs = palloc(sizeof(ItemPointerData) * SpGistPageGetMaxOffset(page));
	in.datums = palloc(sizeof(Datum) * SpGistPageGetMaxOffset(page));

	i = *offset;
	n = 0;

	while( i != InvalidOffsetNumber )
	{
		SpGistLeafTuple	it = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, i));
	
		in.datums[n] = SGLTDATUM(it, state);
		heapPtrs[n] = it->heapPtr; 

		n++;
		i = it->nextOffset;
	}
	in.nTuples = n;

	FunctionCall2(
		&state->picksplitFn,
		PointerGetDatum(&in),
		PointerGetDatum(&out)
	);

	nodes = palloc(sizeof(IndexTuple) * in.nTuples);
	leafBuffers = palloc(sizeof(Buffer) * in.nTuples);
	for(i=0; i<out.nNodes; i++)
	{
		bool	isnull = false;

		nodes[i] = index_form_tuple(state->nodeTupDesc, out.nodeDatums + i, &isnull);
		ItemPointerSetInvalid(&nodes[i]->t_tid);
	}

	for(i=0; i<out.nNodes; i++)
	{
		if (i==0 && BufferGetBlockNumber(buffer) != SPGIST_HEAD_BLKNO)
			leafBuffers[i] = buffer;
		else
			leafBuffers[i] = SpGistNewBuffer(index);

		SpGistInitBuffer(leafBuffers[i], SPGIST_LEAF);
	}

	for(i=0; i<in.nTuples; i++)
	{
		OffsetNumber	newoffset;
		SpGistLeafTuple	it;

		n = out.mapTuplesToNodes[i];
		it = spgFormLeafTuple(state, heapPtrs + i, out.leafTupleDatums[i]);

		it->nextOffset = (ItemPointerIsValid(&nodes[n]->t_tid)) ? 
					ItemPointerGetOffsetNumber(&nodes[n]->t_tid) : InvalidOffsetNumber;

		newoffset = PageAddItem(BufferGetPage(leafBuffers[n]), (Item)it, it->size,
								 InvalidOffsetNumber, false, false);

		ItemPointerSet(&nodes[n]->t_tid, 
						BufferGetBlockNumber(leafBuffers[n]), newoffset);
	}

	for(i=0; i<out.nNodes; i++)
	{
		if (leafBuffers[i] != buffer)
		{
			MarkBufferDirty(leafBuffers[i]);
			UnlockReleaseBuffer(leafBuffers[i]);
		}
	}

	innerTuple = spgFormInnerTuple(state,
									out.hasPrefix, out.prefixDatum,
									out.nNodes, nodes);

	if (parentBuffer != InvalidBuffer && BufferGetBlockNumber(parentBuffer) != SPGIST_HEAD_BLKNO && 
			PageGetFreeSpace(BufferGetPage(parentBuffer)) >= MAXALIGN(innerTuple->size) + MAXALIGN(sizeof(ItemIdData)))
	{
		page = BufferGetPage(parentBuffer);

		*blkno = BufferGetBlockNumber(parentBuffer);
		*offset = PageAddItem(page, (Item)innerTuple, innerTuple->size,
								InvalidOffsetNumber, false, false);
		Assert(*offset != InvalidOffsetNumber);
	}
	else 
	{
		if (parentBuffer == InvalidBuffer) 
		{
			Assert( BufferGetBlockNumber(buffer) == SPGIST_HEAD_BLKNO);
		}
		else
		{
			/* XXX choose inner page with free space */
			buffer = SpGistNewBuffer(index);
		}

		SpGistInitBuffer(buffer, 0);
		page = BufferGetPage(buffer);

		*blkno = BufferGetBlockNumber(buffer);
		*offset = PageAddItem(page, (Item)innerTuple, innerTuple->size,
								InvalidOffsetNumber, false, false);
		Assert(*offset != InvalidOffsetNumber);

		MarkBufferDirty(buffer);
		if (BufferGetBlockNumber(buffer) != SPGIST_HEAD_BLKNO)
			UnlockReleaseBuffer(buffer);
	} 
}

static SpGistInnerTuple 
addNode(SpGistState *state, SpGistInnerTuple tuple, Datum datum)
{
	IndexTuple			node, *nodes;
	bool				isnull = false;
	int					i;


	nodes = palloc(sizeof(IndexTuple) * (tuple->nNodes + 1));
	SGITITERATE(tuple, state, i, node)
		nodes[i] = node;

	nodes[tuple->nNodes] = index_form_tuple(state->nodeTupDesc, &datum, &isnull);

	return spgFormInnerTuple(state,
								!!tuple->hasPrefix, SGITDATUM(tuple, state),
								tuple->nNodes + 1, nodes);
}

void
spgdoinsert(Relation index, SpGistState *state, ItemPointer heapPtr, Datum datum)
{
	Buffer			parentBuffer = InvalidBuffer;
	OffsetNumber	parentOffset = InvalidOffsetNumber;
	int				parentNode = -1;
	OffsetNumber	currentOffset = FirstOffsetNumber;
	BlockNumber	 	blkno = SPGIST_HEAD_BLKNO;
	Datum			leafDatum = datum;
	int				level = 0;

	for(;;) {
		Page		page;
		Buffer 		currentBuffer;


		if (blkno == InvalidBlockNumber)
		{
			/*
			 * create a leaf page
			 */
			currentBuffer = SpGistNewBuffer(index);
			SpGistInitBuffer(currentBuffer, SPGIST_LEAF);
			blkno = BufferGetBlockNumber(currentBuffer);
		}
		else
		{
			currentBuffer = ReadBuffer(index, blkno);
			if (currentBuffer == parentBuffer)
				ReleaseBuffer(currentBuffer);
			else
				LockBuffer(currentBuffer, BUFFER_LOCK_EXCLUSIVE);
		}
		page = BufferGetPage(currentBuffer);

		if (SpGistPageIsLeaf(page))
		{
			SpGistLeafTuple	leafTuple = spgFormLeafTuple(state, heapPtr, leafDatum);

			if (PageGetFreeSpace(page) >= MAXALIGN(leafTuple->size) + MAXALIGN(sizeof(ItemIdData))) {
				leafTuple->nextOffset = currentOffset;
				currentOffset = PageAddItem(page, (Item)leafTuple, leafTuple->size,
													InvalidOffsetNumber, false, false);
				Assert(currentOffset != InvalidOffsetNumber);

				MarkBufferDirty(currentBuffer);
				UnlockReleaseBuffer(currentBuffer);

				if (parentBuffer != InvalidOffsetNumber) {
					SpGistInnerTuple innerTuple;

					page = BufferGetPage(parentBuffer);
					innerTuple = (SpGistInnerTuple) PageGetItem(page,
																PageGetItemId(page, parentOffset));

					updateNodeLink(state, innerTuple, parentNode, blkno, currentOffset);

					MarkBufferDirty(parentBuffer);
					UnlockReleaseBuffer(parentBuffer);
				}

				break; /* go away */
			} else { /* picksplit */
				if (parentBuffer == InvalidBuffer)
				{
					OffsetNumber	j, max = SpGistPageGetMaxOffset(page);

					Assert(blkno == SPGIST_HEAD_BLKNO);

					for(j=FirstOffsetNumber; j<=max; j++) 
					{
						SpGistLeafTuple	lt;

						lt = (SpGistLeafTuple) PageGetItem(page, PageGetItemId(page, j));

						lt->nextOffset = (j==max) ? InvalidOffsetNumber : j+1;
					}

				}
				
				doPickSplit(index, state, currentBuffer, parentBuffer, &blkno, &currentOffset /* in/out */);
				MarkBufferDirty(currentBuffer);

				if (parentBuffer != InvalidOffsetNumber) {
					SpGistInnerTuple innerTuple;

					page = BufferGetPage(parentBuffer);
					innerTuple = (SpGistInnerTuple) PageGetItem(page,
																PageGetItemId(page, parentOffset));

					updateNodeLink(state, innerTuple, parentNode, blkno, currentOffset);

					MarkBufferDirty(parentBuffer);
					UnlockReleaseBuffer(parentBuffer);
				}

				if (parentBuffer != currentBuffer)
					UnlockReleaseBuffer(currentBuffer);
		
				/* simplify for now */
				spgdoinsert(index, state, heapPtr, datum);
				return;
			}
		}
		else /* non leaf */
		{
			SpGistInnerTuple innerTuple;
			spgChooseIn		in;
			spgChooseOut	out;
			IndexTuple		node;
			int				i;

research:
			innerTuple = (SpGistInnerTuple) PageGetItem(page,
														PageGetItemId(page, currentOffset));
			in.datum = datum;
			in.level = level;
			in.hasPrefix = !!innerTuple->hasPrefix;
			in.prefixDatum = SGITDATUM(innerTuple, state);
			in.nNodes = innerTuple->nNodes;
			in.nodeDatums = palloc(sizeof(Datum) * in.nNodes);
			SGITITERATE(innerTuple, state, i, node)
			{
				bool	isnull;

				in.nodeDatums[i] = index_getattr(node, 1, state->nodeTupDesc, &isnull);
			}

			FunctionCall2(
				&state->chooseFn,
				PointerGetDatum(&in),
				PointerGetDatum(&out)
			);

			switch(out.resultType) 
			{
				case spgMatchNode:
					if (parentBuffer != InvalidBuffer && parentBuffer != currentBuffer)
							UnlockReleaseBuffer(parentBuffer);
					parentBuffer = currentBuffer;
					parentOffset = currentOffset;
					parentNode = out.result.matchNode.nodeN;
					level += out.result.matchNode.levelAdd;
					leafDatum = out.result.matchNode.restDatum;
					SGITITERATE(innerTuple, state, i, node)
					{
						if (i == parentNode)
						{
							if (ItemPointerIsValid(&node->t_tid))
							{
								blkno = ItemPointerGetBlockNumber(&node->t_tid);
								currentOffset = ItemPointerGetOffsetNumber(&node->t_tid);
							}
							else
							{
								blkno = InvalidBlockNumber;
								currentOffset = InvalidOffsetNumber;
							}
							break;
						}
					}
					break;
				case spgAddNode:
					{
					SpGistInnerTuple newInnerTuple = addNode(state, innerTuple, out.result.addNode.nodeDatum);
					if (PageGetFreeSpace(page) >= 
							MAXALIGN(newInnerTuple->size) - MAXALIGN(innerTuple->size))
					{
						PageIndexTupleDelete(page, currentOffset);
						PageAddItem(page, (Item)newInnerTuple, newInnerTuple->size,
									currentOffset, false, false);
						
						MarkBufferDirty(currentBuffer);
						/* actually, we will go to spgMatchNode case */
						goto research;
					} else {
						/* move tuple to another page, and update parent */
						Datum 	zero = 0;
						
						Assert(blkno != SPGIST_HEAD_BLKNO);
						/*
						 * ugly but temporary
						 * replace old tuple with empty new. We could not delete
						 * tuple to prevent wrong links from another parents
						 */
						PageIndexTupleDelete(page, currentOffset);
						PageAddItem(page, (Item)&zero, sizeof(zero), currentOffset, false, false);
						MarkBufferDirty(currentBuffer);
						if (parentBuffer != currentBuffer)
							UnlockReleaseBuffer(currentBuffer);

						currentBuffer = SpGistNewBuffer(index);
						SpGistInitBuffer(currentBuffer, 0);

						page = BufferGetPage(currentBuffer);
	
						blkno = BufferGetBlockNumber(currentBuffer);
						currentOffset = PageAddItem(page, (Item)newInnerTuple, newInnerTuple->size,
													InvalidOffsetNumber, false, false);

						MarkBufferDirty(currentBuffer);
						UnlockReleaseBuffer(currentBuffer);

						if (parentBuffer != InvalidOffsetNumber) {
							page = BufferGetPage(parentBuffer);
							innerTuple = (SpGistInnerTuple) PageGetItem(page,
																		PageGetItemId(page, parentOffset));

							updateNodeLink(state, innerTuple, parentNode, blkno, currentOffset);

							MarkBufferDirty(parentBuffer);
							UnlockReleaseBuffer(parentBuffer);
						}

						spgdoinsert(index, state, heapPtr, datum);
						return;
					}
					}
					break;
				case spgSplitTuple:
					{
					SpGistInnerTuple	prefixTuple, postfixTuple;
					IndexTuple			*nodes;
					bool				isnull = false;
			

					node = index_form_tuple(state->nodeTupDesc, 
											&out.result.splitTuple.nodeDatum, &isnull);
					prefixTuple = spgFormInnerTuple(state,  
													out.result.splitTuple.prefixHasPrefix,
													out.result.splitTuple.prefixPrefixDatum,
													1, &node);
					
					nodes = palloc(sizeof(IndexTuple) * innerTuple->nNodes);
					SGITITERATE(innerTuple, state, i, node)
						nodes[i] = node;

					postfixTuple = spgFormInnerTuple(state,
													out.result.splitTuple.postfixHasPrefix,
													out.result.splitTuple.postfixPrefixDatum,
													innerTuple->nNodes, nodes);
					
					PageIndexTupleDelete(page, currentOffset);
					currentOffset = PageAddItem(page, (Item)prefixTuple, 
									prefixTuple->size, currentOffset, false, false);
					Assert( currentOffset != InvalidOffsetNumber);

					innerTuple = (SpGistInnerTuple) PageGetItem(page,
														PageGetItemId(page, currentOffset));

					if (PageGetFreeSpace(page) >= MAXALIGN(postfixTuple->size) +
												  MAXALIGN(sizeof(ItemIdData)) && 
						blkno != SPGIST_HEAD_BLKNO) 
					{
						currentOffset = PageAddItem(page, (Item)postfixTuple, 
										postfixTuple->size, InvalidOffsetNumber, false, false);
						Assert( currentOffset != InvalidOffsetNumber);
					}
					else
					{
						Buffer newBuffer = SpGistNewBuffer(index);

						Assert(!(blkno = SPGIST_HEAD_BLKNO && currentOffset == FirstOffsetNumber));

						SpGistInitBuffer(newBuffer, 0);

						page = BufferGetPage(newBuffer);
	
						blkno = BufferGetBlockNumber(newBuffer);
						currentOffset = PageAddItem(page, (Item)postfixTuple, postfixTuple->size,
													InvalidOffsetNumber, false, false);
						MarkBufferDirty(newBuffer);
						UnlockReleaseBuffer(newBuffer);
					}

					updateNodeLink(state, innerTuple, 0, blkno, currentOffset);
					MarkBufferDirty(currentBuffer);
					UnlockReleaseBuffer(currentBuffer);
					if (currentBuffer != parentBuffer) 
						UnlockReleaseBuffer(parentBuffer);
					spgdoinsert(index, state, heapPtr, datum);
					return;
					}
					break;
				default:
					elog(ERROR, "Unknown choose result");
			}
		}
	} /* for */
}
