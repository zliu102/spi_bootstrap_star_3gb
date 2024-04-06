# contrib/reservoir_sampling/Makefile

MODULE_big = spi_bootstrap_star_3gb
OBJS = spi_bootstrap_star_3gb.o $(WIN32RES)
EXTENSION = spi_bootstrap_star_3gb
DATA = spi_bootstrap_star_3gb--1.0.sql
PGFILEDESC = "spi_bootstrap_star_3gb - binary search for integer arrays"
PG_CFLAGS += -ggdb -O0

REGRESS = spi_bootstrap_star_3gb

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/spi_bootstrap_star_3gb
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
