#include <stdlib.h>
#include <stdio.h>

#include "colorst.h"
#include "colorst_impl.h"

static void colorst_free(colorst *c);
static int  colorst_prepare(colorst *c, const char *query,
	char *message, size_t msgsize);

colorst *
colorst_create(const char *query, int *retcode, char *message, size_t msgsize)
{
	colorst *c;

	c = malloc(sizeof(colorst));
	if (!c) {
		goto malloc_fail;
	}

	if (!colorst_prepare(c, query, message, msgsize)) {
		goto prepare_fail;
	}

	c->free = &colorst_free;
	*retcode = 1;

	return c;

prepare_fail:
	free(c);

malloc_fail:
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
	(void)c;
	struct input i;

	/* init input */
	i.s = query;
	i.eof = 0;
	i.line = i.col = 1;

	i.error = 0;
	i.errmsg = message;
	i.msgsize = msgsize;

	parse_query(&i);

	if (i.error) {
		return 0;
	}

	return 1;
}

