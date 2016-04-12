// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	 http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package acceptance

import (
	"strings"
	"testing"
)

func TestDockerC(t *testing.T) {
	testDockerSuccess(t, "c", []string{"/bin/sh", "-c", strings.Replace(c, "%v", "3", 1)})
	testDockerFail(t, "c", []string{"/bin/sh", "-c", strings.Replace(c, "%v", "a", 1)})
}

const c = `
set -e
cat > main.c << 'EOF'
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <libpq-fe.h>

int
main(int argc, char **argv)
{
	PGconn *conn;
	PGresult *res;
	const char *paramValues[1];
	int paramLengths[1];
	int paramFormats[1];
	uint32_t binaryIntVal;

	conn = PQconnectdb("");
	if (PQstatus(conn) != CONNECTION_OK) {
		fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
		exit(1);
	}

	paramValues[0] = "%v";

	// TODO(mjibson): test with binary results
	res = PQexecParams(conn,
		"SELECT 1, 2 > $1, $1",
		1, /* one param */
		NULL, /* let the backend deduce param type */
		paramValues,
		NULL, /* don't need param lengths since text */
		NULL, /* default to all text params */
		0); /* ask for text results */

	if (PQresultStatus(res) != PGRES_TUPLES_OK) {
		fprintf(stderr, "SELECT failed: %s", PQerrorMessage(conn));
		exit(1);
	}

	char *aptr = PQgetvalue(res, 0, 0);
	char *bptr = PQgetvalue(res, 0, 1);
	char *cptr = PQgetvalue(res, 0, 2);

	if (strcmp(aptr, "1") || strcmp(bptr, "f") || strcmp(cptr, "3")) {
		printf("unexpected: %s, %s, %s\n", aptr, bptr, cptr);
		exit(1);
	}

	return 0;
}
EOF
gcc -std=c99 -I/usr/include/postgresql/ -lpq main.c
./a.out
`
