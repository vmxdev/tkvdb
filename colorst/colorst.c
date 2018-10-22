#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "colorst.h"
#include "colorst_impl.h"


static void colorst_free(colorst *c);
static int  colorst_prepare(colorst *c, const char *query,
	char *message, size_t msgsize);

colorst *
colorst_create(const char *query, tkvdb_tr *tr,
	int *retcode, char *message, size_t msgsize)
{
	colorst *c;
	struct colorst_data *cdt;

	c = malloc(sizeof(colorst));
	if (!c) {
		goto malloc_fail;
	}

	cdt = malloc(sizeof(struct colorst_data));
	if (!cdt) {
		goto malloc_data_fail;
	}
	cdt->tr = tr;
	c->data = cdt;

	if (!colorst_prepare(c, query, message, msgsize)) {
		goto prepare_fail;
	}

	c->free = &colorst_free;
	*retcode = 1;

	return c;

prepare_fail:
malloc_data_fail:
	free(c);
malloc_fail:
	*retcode = 0;
	return NULL;
}

static void
colorst_free(colorst *c)
{
	free(c->data);
	c->data = NULL;

	free(c);
}

static int
colorst_prepare(colorst *c, const char *query, char *message, size_t msgsize)
{
	struct input i;

	/* init input */
	i.s = query;
	i.eof = 0;
	i.line = i.col = 1;

	i.error = 0;
	i.errmsg = message;
	i.msgsize = msgsize;

	i.fl.fields = NULL;
	i.fl.nfields = 0;

	i.data = c->data;

	parse_query(&i);

	if (i.error) {
		return 0;
	}

	return 1;
}

void
mkerror(struct input *i, char *msg)
{
	i->error = 1;

	if (i->line > 1) {
		snprintf(i->errmsg, i->msgsize, "Line %d, col %d: %s",
			i->line, i->col, msg);
	} else {
		snprintf(i->errmsg, i->msgsize, "Col: %d: %s", i->col, msg);
	}
}

