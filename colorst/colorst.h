#ifndef colorst_h_included
#define colorst_h_included

#ifdef __cplusplus
extern "C" {
#endif

typedef struct colorst colorst;

struct colorst
{
	int (*bind_int)(colorst *c, const char *bind, int64_t *data);
	int (*bind_bin)(colorst *c, const char *bind, void *data, size_t len);

	int (*execute)(colorst *c);

	void (*free)(colorst *c);

	void *data;
};

colorst *colorst_create(const char *query,
	int *retcode, char *message, size_t msgsize);

#ifdef __cplusplus
}
#endif

#endif

