#ifndef colorst_impl_h_included
#define colorst_impl_h_included

#include <stdint.h>

#include "colorst.h"

#define TOKEN_MAX_SIZE  512

/* grammar tokens */
enum COLORST_TOKEN
{
	COLORST_BEGIN,
	COLORST_COMMIT,
	COLORST_ROLLBACK,

	COLORST_CREATE,
	COLORST_COLLECTION,

	COLORST_INSERT,
	COLORST_INTO,
	COLORST_VALUE,

	COLORST_SELECT,
	COLORST_FROM,
	COLORST_WHERE,

	COLORST_UPDATE,
	COLORST_SET,

	COLORST_ID,

	COLORST_INT,
	COLORST_DOUBLE,
	COLORST_STRING,
	COLORST_STRING_INCOMPLETE,

	COLORST_PLUS,
	COLORST_MINUS,
	COLORST_MUL,
	COLORST_DIV,
	COLORST_COMMA,
	COLORST_COLON,

	COLORST_CURLY_BRACKET_OPEN,
	COLORST_CURLY_BRACKET_CLOSE
};

/* prefix in DB (casted to uint32_t) */
enum COLORST_PREFIX
{
	COLORST_PREFIX_COLLECTIONS,        /* set of collections */
	COLORST_PREFIX_FIELDS,             /* fields */
	COLORST_PREFIX_COLLROWS,           /* number of rows in collection */
	COLORST_PREFIX_DATA_START = 100    /* data */
};

enum COLORST_FIELD_TYPE
{
	COLORST_FIELD_ID,
	COLORST_FIELD_INT,
	COLORST_FIELD_STRING,
	COLORST_FIELD_OBJECT
};

/* field (name + value) and fields list */
struct field
{
	enum COLORST_FIELD_TYPE type;

	size_t namesize;
	char name[TOKEN_MAX_SIZE];

	size_t valsize;
	char val[TOKEN_MAX_SIZE]; /* FIXME: hmm */
};

struct fields_list
{
	char prefix[TOKEN_MAX_SIZE];

	size_t nfields;
	struct field *fields;
};

struct token
{
	enum COLORST_TOKEN id;

	char str[TOKEN_MAX_SIZE];
	int64_t num;
};

struct colorst_data
{
	tkvdb_tr *tr;

	char collection[TOKEN_MAX_SIZE];
	struct fields_list fl;
};

/* query */
struct input
{
	const char *s;
	int eof;
	int line, col;

	int error;
	char *errmsg;
	size_t msgsize;

	struct token current_token;

	/* pass data to parser */
	struct colorst_data *data;
};


void read_token(struct input *i);
void parse_query(struct input *i);
void mkerror(struct input *i, char *msg);
int  colorst_create_collection(tkvdb_tr *tr, const char *coll_name,
	uint32_t *collidptr, char *msg);
int  colorst_prepare_insert(struct input *i);

#endif

