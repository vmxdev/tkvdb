#include <stdio.h>
#include <stdlib.h>

#include "colorst.h"

int
main()
{
	colorst *c;
	int ret;
	char err[512];

	c = colorst_create("insert into aaa value bbb: \"d\\\"ef\"",
		&ret, err, sizeof(err));

	if (!c) {
		printf("exiting on error\n");
		return EXIT_FAILURE;
	}

	return EXIT_SUCCESS;
}

