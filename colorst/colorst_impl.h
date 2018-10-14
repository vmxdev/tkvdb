#ifndef colorst_impl_h_included
#define colorst_impl_h_included

#include <stdint.h>

#define TOKEN_MAX_SIZE  512

enum COLORST_TOKEN
{
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
	COLORST_COLON
};

struct token
{
	enum COLORST_TOKEN id;

	char str[TOKEN_MAX_SIZE + 1];
	int64_t num;
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
};

void read_token(struct input *i);
void parse_query(struct input *i);

#endif

