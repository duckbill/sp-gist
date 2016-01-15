#include "postgres.h"

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "spgist.h"

PG_FUNCTION_INFO_V1(spg_text_config);
Datum       spg_text_config(PG_FUNCTION_ARGS);
Datum
spg_text_config(PG_FUNCTION_ARGS)
{
	SpGistOpClassProp	*cfg = palloc(sizeof(*cfg));

	cfg->leafType = TEXTOID;
	cfg->prefixType = TEXTOID;
	cfg->nodeType = CHAROID;
	PG_RETURN_POINTER(cfg);
}

static int
commonPrefix(char *a, char *b, int lena, int lenb)
{
	int i = 0;

	while(i < lena && i < lenb && *a == *b)
	{
		a++;
		b++;
		i++;
	}

	return i;
}

PG_FUNCTION_INFO_V1(spg_text_choose);
Datum       spg_text_choose(PG_FUNCTION_ARGS);
Datum
spg_text_choose(PG_FUNCTION_ARGS)
{
	spgChooseIn		*in = (spgChooseIn*)PG_GETARG_POINTER(0);
	spgChooseOut	*out = (spgChooseOut*)PG_GETARG_POINTER(1);
	text			*inText = DatumGetTextP(in->datum);
	int				inSize = VARSIZE(inText) - VARHDRSZ;
	char			nodeChar = '\0';
	int 			i;
	int				common = 0;
	text			*op;

	if (in->hasPrefix)
	{
		text		*prefixText = DatumGetTextP(in->prefixDatum);
		int			prefixSize = VARSIZE(prefixText) - VARHDRSZ;

		common = commonPrefix(VARDATA(inText) + in->level,
							  VARDATA(prefixText),
							  inSize - in->level,
							  prefixSize);

		if (common == prefixSize)
		{
			if (inSize - in->level > common)
				nodeChar = *(VARDATA(inText) + in->level + common);
			else
				nodeChar = '\0';
		}
		else
		{

			out->resultType = spgSplitTuple;
			out->result.splitTuple.levelPrefixAdd = common;

			if (common == 0)
			{
				out->result.splitTuple.prefixHasPrefix = false;
				out->result.splitTuple.nodeDatum = CharGetDatum(*VARDATA(prefixText));
			}
			else
			{
				out->result.splitTuple.prefixHasPrefix = true;
				op = palloc(prefixSize + VARHDRSZ);
				memmove(VARDATA(op), VARDATA(prefixText), common);
				SET_VARSIZE(op, VARHDRSZ + common);
				out->result.splitTuple.prefixPrefixDatum = PointerGetDatum(op);
				out->result.splitTuple.nodeDatum = CharGetDatum(*(VARDATA(prefixText) + common));
			}

			if (prefixSize - common == 1)
			{
				out->result.splitTuple.postfixHasPrefix = false;
			}
			else
			{
				out->result.splitTuple.postfixHasPrefix = true;
				out->result.splitTuple.levelPostfixAdd = prefixSize - common - 1;
				op = palloc(prefixSize + VARHDRSZ);
				memmove(VARDATA(op), VARDATA(prefixText) + common + 1, prefixSize - common - 1);
				SET_VARSIZE(op, VARHDRSZ + prefixSize - common - 1);
				out->result.splitTuple.postfixPrefixDatum = PointerGetDatum(op);
			}

			PG_RETURN_VOID();
		}
	}
	else if (inSize > in->level) 
	{
		nodeChar = *(VARDATA(inText) + in->level);
	}
	else
	{
		nodeChar = '\0';
	}

	for(i=0; i<in->nNodes; i++)
	{
		if (DatumGetChar(in->nodeDatums[i]) == nodeChar)
		{
			out->resultType = spgMatchNode;
			out->result.matchNode.nodeN = i;
			out->result.matchNode.levelAdd = common + 1;

			if (nodeChar == '\0')
			{
				op = palloc(VARHDRSZ);
				SET_VARSIZE(op, VARHDRSZ);
			}
			else
			{
				op = palloc(inSize + VARHDRSZ);
				memmove(VARDATA(op), VARDATA(inText) + in->level + common + 1, 
						inSize - in->level - common - 1);
				SET_VARSIZE(op, VARHDRSZ + inSize - in->level - common - 1);
			}

			out->result.matchNode.restDatum = PointerGetDatum(op);
			PG_RETURN_VOID();
		}
	}

	out->resultType = spgAddNode;
	out->result.addNode.nodeDatum = CharGetDatum(nodeChar);

	PG_RETURN_VOID();
}

typedef struct nodePtr
{
	Datum	d;
	int 	i;
	char	c;
} nodePtr;

static int
cmpNodePtr(const void *a, const void *b)
{
	if ( ((nodePtr*)a)->c == ((nodePtr*)b)->c )
		return 0;
	return ( ((nodePtr*)a)->c > ((nodePtr*)b)->c ) ? 1 : -1;
}

PG_FUNCTION_INFO_V1(spg_text_picksplit);
Datum       spg_text_picksplit(PG_FUNCTION_ARGS);
Datum
spg_text_picksplit(PG_FUNCTION_ARGS)
{
	spgPickSplitIn	*in = (spgPickSplitIn*)PG_GETARG_POINTER(0);
	spgPickSplitOut	*out = (spgPickSplitOut*)PG_GETARG_POINTER(1);
	int 			i, common = 0x7fffffff;
	text			*op;
	nodePtr			*nodes;

	for(i=0; i<in->nTuples; i++)
		in->datums[i] = PointerGetDatum(DatumGetTextP(in->datums[i]));

	for(i=1; i<in->nTuples && common > 0; i++)
	{
		int tmp = commonPrefix(VARDATA(in->datums[0]),
						  		VARDATA(in->datums[i]),
						  		VARSIZE(in->datums[0]) - VARHDRSZ,
						  		VARSIZE(in->datums[i]) - VARHDRSZ);
		if ( tmp < common )
			common = tmp;
	}

	if(common == 0)
	{
		out->hasPrefix = false;
	}
	else
	{
		out->hasPrefix = true;

		op = palloc(common + VARHDRSZ);
		memmove(VARDATA(op), VARDATA(in->datums[0]), common);
		SET_VARSIZE(op, VARHDRSZ + common);
		out->prefixDatum = PointerGetDatum(op);
	}

	nodes = palloc(sizeof(*nodes) * in->nTuples);
	for(i=0; i<in->nTuples; i++)
	{
		if (common == VARSIZE(in->datums[i]) - VARHDRSZ)
			nodes[i].c = '\0';
		else
			nodes[i].c = VARDATA(in->datums[i])[ common ];
		nodes[i].i = i;
		nodes[i].d = in->datums[i];
	}

	qsort(nodes, in->nTuples, sizeof(*nodes), cmpNodePtr); 

	out->nNodes = 0;
	out->nodeDatums = palloc(sizeof(Datum) * in->nTuples);
	out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
	out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

	for(i=0; i<in->nTuples; i++)
	{
		if (i==0 || nodes[i].c != nodes[i-1].c)
		{
			out->nodeDatums[out->nNodes] = CharGetDatum(nodes[i].c);
			out->nNodes++;
		}

		op = palloc(VARSIZE(nodes[i].d));
		if (nodes[i].c == '\0')
		{
			SET_VARSIZE(op, VARHDRSZ);
		}
		else
		{
			memmove(VARDATA(op), VARDATA(nodes[i].d) + common + 1, 
					VARSIZE(nodes[i].d) - VARHDRSZ - common - 1);
			SET_VARSIZE(op, VARSIZE(nodes[i].d) - common - 1);
		}

		out->leafTupleDatums[ nodes[i].i ] = PointerGetDatum(op);
		out->mapTuplesToNodes[ nodes[i].i ] = out->nNodes - 1;
	}

	Assert(out->nNodes > 1);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spg_text_leaf_consistent);
Datum       spg_text_leaf_consistent(PG_FUNCTION_ARGS);
Datum
spg_text_leaf_consistent(PG_FUNCTION_ARGS)
{
	int		level = PG_GETARG_INT32(0);
	text	*query = PG_GETARG_TEXT_P(1);
	text	*datum = PG_GETARG_TEXT_P(2);

	if (VARSIZE(query) != VARSIZE(datum) + level)
		PG_RETURN_BOOL(false);

	if (VARSIZE(datum) == VARHDRSZ)
		PG_RETURN_BOOL(true);

	if (memcmp(VARDATA(datum), VARDATA(query) + level, VARSIZE(datum) - VARHDRSZ) == 0)
		PG_RETURN_BOOL(true);

	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(spg_text_inner_consistent);
Datum       spg_text_inner_consistent(PG_FUNCTION_ARGS);
Datum
spg_text_inner_consistent(PG_FUNCTION_ARGS)
{
	spgInnerConsistentIn	*in = (spgInnerConsistentIn*)PG_GETARG_POINTER(0);
	spgInnerConsistentOut	*out = (spgInnerConsistentOut*)PG_GETARG_POINTER(1);
	text					*inText;
	int						inSize;
	int						common = 0, i;
	char					nodeChar = '\0';

	inText = DatumGetTextP(in->query);
	inSize = VARSIZE(inText) - VARHDRSZ;

	if (in->hasPrefix)
	{
		text		*prefixText = DatumGetTextP(in->prefixDatum);
		int			prefixSize = VARSIZE(prefixText) - VARHDRSZ;

		common = commonPrefix(VARDATA(inText) + in->level,
							  VARDATA(prefixText),
							  inSize - in->level,
							  prefixSize);

		if (common == prefixSize)
		{
			if (inSize - in->level > common)
				nodeChar = *(VARDATA(inText) + in->level + common);
			else
				nodeChar = '\0';
		}
		else
		{
			out->nNodes = 0;
			PG_RETURN_VOID();
		}
	}
	else if (inSize > in->level) 
	{
		nodeChar = *(VARDATA(inText) + in->level);
	}
	else
	{
		nodeChar = '\0';
	}

	out->levelAdd = common + 1;

	out->nodeNumbers = palloc(sizeof(int));
	out->nNodes = 0;

	for(i=0; i<in->nNodes; i++)
		if ((inSize - in->level == 0 && DatumGetChar(in->nodeDatums[i]) == '\0') ||
			*(VARDATA(inText) + in->level + common) == DatumGetChar(in->nodeDatums[i]))
		{
			out->nodeNumbers[0] = i;
			out->nNodes++;
			break;
		}

	PG_RETURN_VOID();
}

