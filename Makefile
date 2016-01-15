MODULE_big = spgist
OBJS = spgutils.o spginsert.o spgscan.o spgvacuum.o spgcost.o \
	spgdoinsert.o spgtextproc.o spgquadtreeproc.o

EXTENSION = spgist
DATA = spgist--1.0.sql
REGRESS = spgist

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/spgist
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

