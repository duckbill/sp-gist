#include "postgres.h"

#include "fmgr.h"
#include "catalog/pg_type.h"
#include "utils/geo_decls.h"
#include "spgist.h"

PG_FUNCTION_INFO_V1(spg_quad_config);
Datum       spg_quad_config(PG_FUNCTION_ARGS);
Datum
spg_quad_config(PG_FUNCTION_ARGS)
{
	SpGistOpClassProp	*cfg = palloc(sizeof(*cfg));

	cfg->leafType = POINTOID;
	cfg->prefixType = POINTOID;
	cfg->nodeType = INT2OID;
	PG_RETURN_POINTER(cfg);
}

#define SPTEST(f, c, t)					\
	DatumGetBool(DirectFunctionCall2(	\
				point_##f,				\
				PointPGetDatum(c),		\
				PointPGetDatum(t)))		\


static int2
getQuadrant(Point *centroid, Point *tst)
{
	if ( (SPTEST(above, tst, centroid) || SPTEST(horiz, tst, centroid)) &&
		 (SPTEST(right, tst, centroid) || SPTEST(vert, tst, centroid)) )
		return 1;

	if ( SPTEST(below, tst, centroid) &&
		 (SPTEST(right, tst, centroid) || SPTEST(vert, tst, centroid)) )
		return 2;

	if ( (SPTEST(below, tst, centroid) || SPTEST(horiz, tst, centroid))&&
		 SPTEST(left, tst, centroid) )
		return 3;

	if ( SPTEST(above, tst, centroid) && SPTEST(left, tst, centroid) )
		return 4;

	elog(ERROR, "getQuadrant: could not be here");
	return 0;
}


PG_FUNCTION_INFO_V1(spg_quad_choose);
Datum       spg_quad_choose(PG_FUNCTION_ARGS);
Datum
spg_quad_choose(PG_FUNCTION_ARGS)
{
	spgChooseIn		*in = (spgChooseIn*)PG_GETARG_POINTER(0);
	spgChooseOut	*out = (spgChooseOut*)PG_GETARG_POINTER(1);
	Point			*inPoint = DatumGetPointP(in->datum),
					*centroid;
	Point			*d;

	Assert(in->hasPrefix);

	centroid = DatumGetPointP(in->prefixDatum);

	Assert(in->nNodes == 4);

	d = palloc(sizeof(*d));
	*d = *inPoint;

	out->resultType = spgMatchNode;
	out->result.matchNode.nodeN = getQuadrant(centroid, inPoint) - 1;
	out->result.matchNode.levelAdd = 0;
	out->result.matchNode.restDatum = PointPGetDatum(d);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spg_quad_picksplit);
Datum       spg_quad_picksplit(PG_FUNCTION_ARGS);
Datum
spg_quad_picksplit(PG_FUNCTION_ARGS)
{
	spgPickSplitIn	*in = (spgPickSplitIn*)PG_GETARG_POINTER(0);
	spgPickSplitOut	*out = (spgPickSplitOut*)PG_GETARG_POINTER(1);
	int 			i;
	Point			*centroid;
	int				nQuadrants[] = {0, 0, 0, 0};

	centroid = palloc(sizeof(*centroid));
	for(i=0; i<in->nTuples; i++)
	{
		Point	*p = DatumGetPointP(in->datums[i]);

		if (i==0)
		{
			*centroid = *p;
		}
		else
		{
			centroid->x += p->x;
			centroid->y += p->y;
		}
	}

	centroid->x /= (double)in->nTuples;
	centroid->y /= (double)in->nTuples;

	out->hasPrefix = true;
	out->prefixDatum = PointPGetDatum(centroid);

	out->nNodes = 4;
	out->nodeDatums = palloc(sizeof(Datum) * 4);
	for(i=0; i<4; i++)
		out->nodeDatums[i] = Int16GetDatum((int2)i);
	out->mapTuplesToNodes = palloc(sizeof(int) * in->nTuples);
	out->leafTupleDatums = palloc(sizeof(Datum) * in->nTuples);

	for(i=0; i<in->nTuples; i++)
	{
		Point   *p = DatumGetPointP(in->datums[i]), *op;
		int2	quadrant = getQuadrant(centroid, p) - 1;

		op = palloc(sizeof(*op));
		*op = *p;

		out->leafTupleDatums[ i ] = PointPGetDatum(op);
		out->mapTuplesToNodes[ i ] = quadrant;
		nQuadrants[quadrant]++;
	}

	for(i=0; i<4; i++)
	{
		if (nQuadrants[i] == in->nTuples)
			elog(ERROR, "Could not support some strange corner cases, will fix in future");
	}

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(spg_quad_leaf_consistent);
Datum       spg_quad_leaf_consistent(PG_FUNCTION_ARGS);
Datum
spg_quad_leaf_consistent(PG_FUNCTION_ARGS)
{
	/*int		level = PG_GETARG_INT32(0);*/
	Point	*query = PG_GETARG_POINT_P(1);
	Point	*datum = PG_GETARG_POINT_P(2);
	bool	res;

	res = SPTEST(eq, datum, query);

	PG_RETURN_BOOL(res);
}

PG_FUNCTION_INFO_V1(spg_quad_inner_consistent);
Datum       spg_quad_inner_consistent(PG_FUNCTION_ARGS);
Datum
spg_quad_inner_consistent(PG_FUNCTION_ARGS)
{
	spgInnerConsistentIn	*in = (spgInnerConsistentIn*)PG_GETARG_POINTER(0);
	spgInnerConsistentOut	*out = (spgInnerConsistentOut*)PG_GETARG_POINTER(1);
	Point					*query,
							*centroid;

	query = DatumGetPointP(in->query);
	Assert(in->hasPrefix);
	centroid = DatumGetPointP(in->prefixDatum);


	out->levelAdd = 0;

	out->nodeNumbers = palloc(sizeof(int));
	out->nNodes = 1;
	out->nodeNumbers[0] = getQuadrant(centroid, query) - 1;

	PG_RETURN_VOID();
}

