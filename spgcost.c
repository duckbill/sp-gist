#include "postgres.h"

#include "fmgr.h"
#include "optimizer/cost.h"
#include "utils/selfuncs.h"

#include "spgist.h"

PG_FUNCTION_INFO_V1(spgcostestimate);
Datum       spgcostestimate(PG_FUNCTION_ARGS);
Datum
spgcostestimate(PG_FUNCTION_ARGS)
{

	DirectFunctionCall9(
		gistcostestimate,
		PG_GETARG_DATUM(0),
		PG_GETARG_DATUM(1),
		PG_GETARG_DATUM(2),
		PG_GETARG_DATUM(3),
		PG_GETARG_DATUM(4),
		PG_GETARG_DATUM(5),
		PG_GETARG_DATUM(6),
		PG_GETARG_DATUM(7),
		PG_GETARG_DATUM(8)
	);

	PG_RETURN_VOID();
}
