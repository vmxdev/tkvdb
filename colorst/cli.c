#include <stdio.h>
#include <stdlib.h>
#include <readline/readline.h>
#include <readline/history.h>

#include "colorst.h"

int
main()
{
	colorst *c;
	int ret;
	char err[512];
	char *input;
	tkvdb_tr *tr = NULL;

	tr = tkvdb_tr_create(NULL, NULL);
	tr->begin(tr);
	for(;;) {
		input = readline("$ ");
		if (!input) {
			break;
		}
		add_history(input);

		c = colorst_create(input, tr, &ret, err, sizeof(err));

		if (!c) {
			printf("Error: %s\n", err);
		} else {
			c->free(c);
		}
		free(input);
	}

	return EXIT_SUCCESS;
}

