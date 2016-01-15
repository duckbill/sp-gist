SET search_path='public';

CREATE OR REPLACE FUNCTION spgbuild(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgbuildempty(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spginsert(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgbeginscan(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgrescan(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgendscan(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgmarkpos(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgrestrpos(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spggetbitmap(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgbulkdelete(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgvacuumcleanup(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgcostestimate(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spgoptions(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

INSERT INTO pg_am (
    amname,
	amstrategies,
	amsupport,
	amcanorder,
	amcanorderbyop,
	amcanbackward,
	amcanunique,
	amcanmulticol,
	amoptionalkey,
	amsearchnulls,
	amstorage,
	amclusterable,
	ampredlocks,
	amkeytype,
	aminsert,
	ambeginscan,
	amgettuple,
	amgetbitmap,
	amrescan,
	amendscan,
	ammarkpos,
	amrestrpos,
	ambuild,
	ambuildempty,
	ambulkdelete,
	amvacuumcleanup,
	amcostestimate,
	amoptions
) VALUES (
	'spgist',           --amname
	0,                  --amstrategies
	5,                  --amsupport
	'f',                --amcanorder
	'f',                --amcanorderbyop
	'f',                --amcanbackward
	'f',                --amcanunique
	'f',                --amcanmulticol
	'f',                --amoptionalkey
	'f',                --amsearchnulls
	'f',                --amstorage
	'f',                --amclusterable
	'f',                --ampredlocks
	2281,               --amkeytype
	'spginsert',        --aminsert
	'spgbeginscan',     --ambeginscan
	'-',                --amgettuple
	'spggetbitmap',     --amgetbitmap
	'spgrescan',        --amrescan
	'spgendscan',       --amendscan
	'spgmarkpos',       --ammarkpos
	'spgrestrpos',      --amrestrpos
	'spgbuild',         --ambuild
	'spgbuildempty',     --ambuildempty
	'spgbulkdelete',    --ambulkdelete
	'spgvacuumcleanup', --amvacuumcleanup
	'spgcostestimate',  --amcostestimate
	'spgoptions' 		--amoptions
);

--Text opclass

CREATE OR REPLACE FUNCTION spg_text_config(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_text_choose(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_text_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_text_leaf_consistent(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_text_inner_consistent(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OPERATOR CLASS text_ops DEFAULT
FOR TYPE text USING spgist
AS
        OPERATOR        1       = (text, text),
		FUNCTION        1       spg_text_config(internal),
		FUNCTION        2       spg_text_choose(internal, internal),
		FUNCTION        3       spg_text_picksplit(internal, internal),
		FUNCTION        4       spg_text_leaf_consistent(internal, internal, internal),
		FUNCTION		5		spg_text_inner_consistent(internal, internal)
;

--Quadtree opclass

CREATE OR REPLACE FUNCTION spg_quad_config(internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_quad_choose(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_quad_picksplit(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_quad_leaf_consistent(internal, internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OR REPLACE FUNCTION spg_quad_inner_consistent(internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE OPERATOR CLASS point_quadtree_ops DEFAULT
FOR TYPE point USING spgist
AS
        OPERATOR        1       ~= (point, point),
		FUNCTION        1       spg_quad_config(internal),
		FUNCTION        2       spg_quad_choose(internal, internal),
		FUNCTION        3       spg_quad_picksplit(internal, internal),
		FUNCTION        4       spg_quad_leaf_consistent(internal, internal, internal),
		FUNCTION		5		spg_quad_inner_consistent(internal, internal)
;

--debug

CREATE OR REPLACE FUNCTION spgstat(text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C;

