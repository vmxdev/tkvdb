#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>

#include "colorst_impl.h"

static int
is_idsym(int c)
{
	if (isalnum(c)) return 1;
	if (c == '_') return 1;
	if (c == '$') return 1;

	return 0;
}

void
read_token(struct input *i)
{

#define CHECK_EOF()               \
	if (*(i->s) == '\0') {    \
		i->eof = 1;       \
		return;           \
	}

#define BREAK_IF_EOF()            \
	if (*(i->s) == '\0') {    \
		break;            \
	}

#define NEXT_SYMBOL()             \
do {                              \
	i->s++;                   \
	i->col++;                 \
} while (0)

again:
	/* skip white space */
	for (;;) {
		CHECK_EOF();
		if (*(i->s) == '\n') {
			i->line++;
			i->col = 1;
		}
		if (isspace(*(i->s))) {
			NEXT_SYMBOL();
			CHECK_EOF();
		} else {
			break;
		}
	}

	/* skip comment */
	if (*(i->s) == '/') {

#define IF_NEXT(SYM)           \
	NEXT_SYMBOL();         \
	if (*(i->s) == SYM)

		IF_NEXT('*') {
			for (;;) {
				IF_NEXT('\n') {
					i->line++;
				}
				if (*(i->s) != '*') {
					continue;
				}
				IF_NEXT('/') {
					i->s++;
					goto again;
				}
				i->s--;
			}
		}
		i->s--;
	}


#define SCAN_UNTIL(COND)                                 \
	do {                                             \
		i->current_token.str[l] = *(i->s);       \
		l++;                                     \
		if (l > sizeof(i->current_token.str)) {  \
			mkerror(i, "Token is too big");  \
			return;                          \
		}                                        \
		i->s++;                                  \
		i->col++;                                \
	} while (COND)


	/* keywords and id's */
	if (isalpha(*(i->s))) {
                size_t l = 0;

		SCAN_UNTIL(is_idsym(*(i->s)));

		i->current_token.str[l] = '\0';
#define SCAN_STRING(STR, TOKEN) else if                  \
	(strcasecmp(STR, i->current_token.str) == 0) {   \
		i->current_token.id = TOKEN;             \
	}
		if (0) {}
		SCAN_STRING("begin", COLORST_BEGIN)
		SCAN_STRING("commit", COLORST_COMMIT)
		SCAN_STRING("rollback", COLORST_ROLLBACK)

		SCAN_STRING("create", COLORST_CREATE)
		SCAN_STRING("collection", COLORST_COLLECTION)

		SCAN_STRING("insert", COLORST_INSERT)
		SCAN_STRING("into", COLORST_INTO)
		SCAN_STRING("value", COLORST_VALUE)
		else {
			i->current_token.id = COLORST_ID;
		}
#undef SCAN_STRING
		return;
	}

	/* number */
	if (isdigit(*(i->s))) {
		size_t l = 0;

		SCAN_UNTIL(isdigit(*(i->s)));

		if (is_idsym(*(i->s))) {
			mkerror(i, "Incorrect token");
			return;
		}

		i->current_token.str[l] = '\0';
		i->current_token.num = atoi(i->current_token.str);
		i->current_token.id = COLORST_INT;
		return;
	}

	/* string */
	if (*(i->s) == '\"') {
		size_t l = 0;
		int escaped = 0;

		i->current_token.id = COLORST_STRING_INCOMPLETE;
		NEXT_SYMBOL();
		CHECK_EOF();
		while (!((*(i->s) == '\"') && !escaped)) {
			if (*(i->s) == '\\') {
				NEXT_SYMBOL();
				BREAK_IF_EOF();
				escaped = 1;
			}

			i->current_token.str[l] = *(i->s);
			l++;
			if (l > sizeof(i->current_token.str)) {
				mkerror(i, "Token is too big");
				return;
			}
			NEXT_SYMBOL();
			BREAK_IF_EOF();
		}

		if (*(i->s) != '\"') {
			mkerror(i, "Incorrect string token");
			return;
		}

		i->current_token.str[l] = '\0';
		i->current_token.id = COLORST_STRING;
		NEXT_SYMBOL();
		return;
	}

#define SINGLE_SYM_TOKEN(SYM, ID)               \
	if (*(i->s) == SYM) {                   \
		i->current_token.str[0] = SYM;  \
		i->current_token.str[1] = '\0'; \
		i->current_token.id = ID;       \
		NEXT_SYMBOL();                  \
		return;                         \
	}

	SINGLE_SYM_TOKEN(',', COLORST_COMMA);
	SINGLE_SYM_TOKEN('-', COLORST_MINUS);
	SINGLE_SYM_TOKEN('+', COLORST_PLUS);
	SINGLE_SYM_TOKEN(':', COLORST_COLON);
	SINGLE_SYM_TOKEN('{', COLORST_CURLY_BRACKET_OPEN);
	SINGLE_SYM_TOKEN('}', COLORST_CURLY_BRACKET_CLOSE);

#undef SINGLE_SYM_TOKEN
#undef SCAN_UNTIL
#undef CHECK_EOF
#undef NEXT_SYMBOL
#undef IF_NEXT
	{
		char msg[128];

		snprintf(msg, sizeof(msg), "Unrecognized token '%c'", i->s[0]);
		mkerror(i, msg);
		return;
	}
}


