#ifndef colorst_h_included
#define colorst_h_included

#include "tkvdb.h"

#ifdef __cplusplus
extern "C" {
#endif

/* statement */
typedef struct colorst colorst;

struct colorst
{
	int (*bind_int)(colorst *c, const char *bind, int64_t *data);
	int (*bind_bin)(colorst *c, const char *bind, void *data, size_t len);

	int (*execute)(colorst *c);

	void (*free)(colorst *c);

	void *data;
};

colorst *colorst_create(const char *query, tkvdb_tr *tr,
	int *retcode, char *msg, size_t msgsize);

#ifdef __cplusplus
}
#endif

#endif

