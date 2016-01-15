#ifndef _SPGIST_H_
#define _SPGIST_H_

#include "access/genam.h"
#include "access/itup.h"
#include "access/xlog.h"
#include "fmgr.h"


#define SPGIST_PROP_PROC		1
#define SPGIST_CHOOSE_PROC		2
#define SPGIST_PICKSPLIT_PROC	3
#define SPGIST_LEAFCONS_PROC	4
#define SPGIST_INNERCONS_PROC	5
#define SPGISTNProc         	5

typedef struct SpGistOpClassProp 
{
	Oid				leafType;
	Oid				prefixType;
	Oid				nodeType;
} SpGistOpClassProp;

typedef struct SpGistPageOpaqueData
{
	uint16         nParents;
	uint16         flags;
} SpGistPageOpaqueData;

typedef SpGistPageOpaqueData *SpGistPageOpaque;

#define SPGIST_META     	(1<<0)
#define SPGIST_DELETED  	(1<<1)
#define SPGIST_LEAF   		(1<<2)
#define SPGIST_HAS_DELETED  (1<<3)

#define SpGistPageGetOpaque(page) ( (SpGistPageOpaque) PageGetSpecialPointer(page) )
#define SpGistPageGetMaxOffset(page) PageGetMaxOffsetNumber(page)
#define SpGistPageIsMeta(page) ( SpGistPageGetOpaque(page)->flags & SPGIST_META)
#define SpGistPageIsDeleted(page) ( SpGistPageGetOpaque(page)->flags & SPGIST_DELETED)
#define SpGistPageSetDeleted(page)    ( SpGistPageGetOpaque(page)->flags |= SPGIST_DELETED)
#define SpGistPageSetNonDeleted(page) ( SpGistPageGetOpaque(page)->flags &= ~SPGIST_DELETED)
#define SpGistPageIsLeaf(page) ( SpGistPageGetOpaque(page)->flags & SPGIST_LEAF)
#define SpGistPageSetLeaf(page)    ( SpGistPageGetOpaque(page)->flags |= SPGIST_LEAF)
#define SpGistPageSetInner(page) ( SpGistPageGetOpaque(page)->flags &= ~SPGIST_LEAF)
#define SpGistPageGetData(page)      (  (SpGistLeafTuple*)PageGetContents(page) )

#define SPGIST_METAPAGE_BLKNO    (0)
#define SPGIST_HEAD_BLKNO        (1)

typedef BlockNumber FreeBlockNumberArray[
			MAXALIGN_DOWN(
				BLCKSZ -
					SizeOfPageHeaderData -
					MAXALIGN(sizeof(SpGistPageOpaqueData)) -
					/* header of SpGistMetaPageData struct */
					MAXALIGN(sizeof(uint16) * 2 + sizeof(uint32))
			) / sizeof(BlockNumber)
	];

typedef struct SpGistMetaPageData
{
	uint32                  magickNumber;
	uint16                  nStart;
	uint16                  nEnd;
	FreeBlockNumberArray    notFullPage;
} SpGistMetaPageData;

#define SPGIST_MAGICK_NUMBER (0xBA0BABED)

#define SpGistMetaBlockN     (sizeof(FreeBlockNumberArray) / sizeof(BlockNumber))
#define SpGistPageGetMeta(p) \
    ((SpGistMetaPageData *) PageGetContents(p))

typedef struct SpGistTypeDesc 
{
	Oid		type;
	bool	attbyval;
	int16	attlen;
} SpGistTypeDesc;

typedef struct SpGistState
{
	SpGistOpClassProp	prop;
	SpGistTypeDesc		attType;
	SpGistTypeDesc		attNodeType;
	SpGistTypeDesc		attPrefixType; /* optional */
	
	FmgrInfo			chooseFn;
	FmgrInfo			picksplitFn;
	FmgrInfo			leafConsistentFn;
	FmgrInfo			innerConsistentFn;

	TupleDesc			nodeTupDesc;
} SpGistState;

typedef struct SpGistScanOpaqueData
{
	SpGistState  	state;
	MemoryContext 	tempCxt;
} SpGistScanOpaqueData;

typedef SpGistScanOpaqueData *SpGistScanOpaque;

/*
 * node/tuple types
 * Inner tuple layout:
 * (bit of presence of prefix)(n nodes)(size)[(prefix)](array of nodes)
 */
typedef struct SpGistInnerTupleData
{
	unsigned int	hasPrefix:1, /* could be true for TreeShrink */ 
					nNodes:15,
					size:16;
	char			data[1]; /* variable size */
} SpGistInnerTupleData;
typedef SpGistInnerTupleData *SpGistInnerTuple;

#define SGITHDRSZ			MAXALIGN(offsetof(SpGistInnerTupleData, data))
#define  _SGITDATA(x)		( ((char*)(x)) + SGITHDRSZ )
#define SGITDATAPTR(x)  	( ((x)->hasPrefix) ? _SGITDATA(x) : NULL )
#define SGITDATUM(x, s)		( ((x)->hasPrefix) ? \
								( \
									((s)->attPrefixType.attbyval) ? \
									( *(Datum*)_SGITDATA(x) ) \
									: \
									PointerGetDatum(_SGITDATA(x)) \
								) \
								: (Datum)0 )
#define SGITNODEPTR(x, s) 	( (IndexTuple)( \
								_SGITDATA(x) + \
								( ((x)->hasPrefix) ? \
									getTypeLength( &(s)->attPrefixType, PointerGetDatum(_SGITDATA(x)) ) \
									: 0 \
								) \
							) )

#define SGITITERATE(x, s, i, it)	\
	for((i) = 0, (it)= SGITNODEPTR((x),(s)) ; \
		(i) < (x)->nNodes; \
		(i)++, (it) = (IndexTuple)(((char*)(it)) + IndexTupleSize(it))) 


/*
 * indexed value could be empty (not a NULL) if it's
 * fully encoded in a tree path
 */
typedef struct SpGistLeafTupleData
{
	ItemPointerData	heapPtr;
	OffsetNumber	nextOffset;
	unsigned short 	size;
	char			data[1]; /* variable size */
} SpGistLeafTupleData;
typedef SpGistLeafTupleData *SpGistLeafTuple;

#define SGLTHDRSZ			MAXALIGN(offsetof(SpGistLeafTupleData, data))
#define SGLTDATAPTR(x)  	( ((char*)(x)) + SGLTHDRSZ )
#define SGLTDATUM(x, s)		( ((s)->attType.attbyval) ? \
								( *(Datum*)SGLTDATAPTR(x) ) \
								: \
								PointerGetDatum(SGLTDATAPTR(x)) \
							)

/*
 * interface struct
 */

typedef struct spgChooseIn
{
	Datum	datum; /* full indexing datum */
	int		level;

	/*
	 * Inner tuple data
	 */
	bool	hasPrefix;
	Datum	prefixDatum;

	int		nNodes;
	Datum	*nodeDatums;
} spgChooseIn;

typedef enum spgChooseResultType
{
	spgMatchNode = 0,
	spgAddNode,
	spgSplitTuple
} spgChooseResultType;

typedef struct spgChooseOut
{
	spgChooseResultType	resultType;
	union {
		struct {
			int		nodeN;
			int		levelAdd;
			Datum	restDatum;
		} matchNode;
		struct {
			Datum	nodeDatum;
		} addNode;
		struct {
			/* new inner tuple with one node */
			int		levelPrefixAdd;
			bool	prefixHasPrefix;
			Datum	prefixPrefixDatum;
			Datum	nodeDatum;

			/* new next inner tuple with all old nodes */ 
			int		levelPostfixAdd;
			bool	postfixHasPrefix;
			Datum	postfixPrefixDatum;
		} splitTuple;
	} result; 
} spgChooseOut;

typedef struct spgPickSplitIn
{
	int		nTuples;
	Datum	*datums;
} spgPickSplitIn;

typedef struct spgPickSplitOut
{
	bool	hasPrefix;
	Datum	prefixDatum;

	int		nNodes;
	Datum	*nodeDatums;

	int		*mapTuplesToNodes;
	Datum	*leafTupleDatums;
} spgPickSplitOut;

typedef struct spgInnerConsistentIn
{
	Datum	query;

	int		level;

	bool	hasPrefix;
	Datum	prefixDatum;

	int		nNodes;
	Datum	*nodeDatums;
} spgInnerConsistentIn;

typedef struct spgInnerConsistentOut
{
	int		nNodes;
	int		levelAdd; /* including node's levels */
	int		*nodeNumbers;
} spgInnerConsistentOut;

/* spgutils.h */
void initSpGistState(SpGistState *state, Relation index);
Buffer SpGistNewBuffer(Relation index);
void SpGistInitBuffer(Buffer b, uint16 f);
void SpGistInitPage(Page page, uint16 f, Size pageSize);
void SpGistInitMetabuffer(Buffer b, Relation index);

unsigned int getTypeLength(SpGistTypeDesc *att, Datum datum);
SpGistLeafTuple spgFormLeafTuple(SpGistState *state, ItemPointer heapPtr, Datum datum);
SpGistInnerTuple spgFormInnerTuple(SpGistState *state, bool hasPrefix, Datum prefix, 
									int nNodes, IndexTuple *nodes);

void spgdoinsert(Relation index, SpGistState *state, ItemPointer heapPtr, Datum datum);
#endif
