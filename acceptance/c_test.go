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
	testDockerSuccess(t, "c", []string{"/bin/sh", "-c", strings.Replace(c, "%v", "SELECT $1, $2, $3, $4, $5, $6", 1)})
	testDockerFail(t, "c", []string{"/bin/sh", "-c", strings.Replace(c, "%v", "SELECT 1", 1)})
}

const c = `
set -e
cat > main.c << 'EOF'
#include <libpq-fe.h>
#include <libpqtypes.h>
#include <limits.h>
#include <string.h>
#include <sys/param.h>

int main(int argc, char const *argv[]) {
	PGconn *conn = PQconnectdb("");
	if (PQstatus(conn) != CONNECTION_OK) {
		fprintf(stderr, "Connection to database failed: %s", PQerrorMessage(conn));
		return 1;
	}

	/* Always call first on any conn that is to be used with libpqtypes */
	PQinitTypes(conn);

	const char *query = "%v";

	PGparam *param = PQparamCreate(conn);

	PGbool b = 1;
	if (!PQputf(param, "%bool", b)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

	char bytes[] = "hello";
	PGbytea bytea;
	bytea.len = sizeof(bytes);
	bytea.data = bytes;
	if (!PQputf(param, "%bytea", &bytea)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

  // // '1401-01-19 BC'
  // PGdate date;
  // date.isbc = 1;
  // date.year = 1401;
  // date.mon  = 0;
  // date.mday = 19;
  // if (!PQputf(param, "%date", &date)) {
  // 	fprintf(stderr, "ERROR: %s\n", PQgeterror());
  // 	return 1;
  // }

	PGnumeric numeric = "-1728718718271827121233.1212121212";
	if (!PQputf(param, "%numeric", numeric)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

	PGfloat8 f8 = 123456.789;
	if (!PQputf(param, "%float8", f8)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

	PGint8 i8 = INT_MAX;
	if (!PQputf(param, "%int8", i8)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

 //  // "20 years 8 months 9 hours 10 mins 15 secs 123456 usecs"
 //  PGinterval interval;
 //  interval.years = 20;
 //  interval.mons  = 8;
 //  interval.days  = 0; // not used, set to 0
 //  interval.hours = 9;
 //  interval.mins  = 10;
 //  interval.secs  = 15;
 //  interval.usecs = 123456;
 //  if (!PQputf(param, "%interval", &interval)) {
 //  	fprintf(stderr, "ERROR: %s\n", PQgeterror());
 //  	return 1;
 //  }

	PGtext text = "foobar";
	if (!PQputf(param, "%text", text)) {
		fprintf(stderr, "ERROR: %s\n", PQgeterror());
		return 1;
	}

 //  // '2000-01-19 10:41:06'
 //  PGtimestamp ts;
 //  ts.date.isbc   = 0;
 //  ts.date.year   = 2000;
 //  ts.date.mon    = 0;
 //  ts.date.mday   = 19;
 //  ts.time.hour   = 10;
 //  ts.time.min    = 41;
 //  ts.time.sec    = 6;
 //  ts.time.usec   = 0;
 //  if (!PQputf(param, "%timestamp", &ts)) {
 //  	fprintf(stderr, "ERROR: %s\n", PQgeterror());
 //  	return 1;
 //  }

 //  // '2000-01-19 10:41:06-05'
 //  PGtimestamp tstz;
 //  tstz.date.isbc   = 0;
 //  tstz.date.year   = 2000;
 //  tstz.date.mon    = 0;
 //  tstz.date.mday   = 19;
 //  tstz.time.hour   = 10;
 //  tstz.time.min    = 41;
 //  tstz.time.sec    = 6;
 //  tstz.time.usec   = 0;
 //  tstz.time.gmtoff = -18000;
 //  if (!PQputf(param, "%timestamptz", &tstz)) {
 //  	fprintf(stderr, "ERROR: %s\n", PQgeterror());
 //  	return 1;
 //  }

	// resultFormat: 0 for text, 1 for binary.
	for (int resultFormat = 0; resultFormat <= 1; ++resultFormat) {
			PGresult *result = PQparamExec(conn, param, query, resultFormat);
			if(!result) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}

			PGbool recvb;
			if (!PQgetf(result, 0, "%bool", 0, &recvb)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (recvb != b) {
				fprintf(stderr, "expected: %d, got: %d\n", b, recvb);
				return 1;
			}

			PGbytea recvbytea;
			if (!PQgetf(result, 0, "%bytea", 1, &recvbytea)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (memcmp(recvbytea.data, bytea.data, MIN(recvbytea.len, bytea.len)) != 0) {
				fprintf(stderr, "expected (%d bytes): ", bytea.len);
				for (int i = 0; i < bytea.len; ++i) {
					fprintf(stderr, "%c", bytea.data[i]);
				}
				fprintf(stderr, " got (%d bytes): ", recvbytea.len);
				for (int i = 0; i < recvbytea.len; ++i) {
					fprintf(stderr, "%c", recvbytea.data[i]);
				}
				fprintf(stderr, "\n");
				return 1;
			}

			PGnumeric recvnumeric;
			if (!PQgetf(result, 0, "%numeric", 2, &recvnumeric)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (strcmp(recvnumeric, numeric)) {
				fprintf(stderr, "expected: %s, got: %s\n", numeric, recvnumeric);
				return 1;
			}

			PGfloat8 recvf8;
			if (!PQgetf(result, 0, "%float8", 3, &recvf8)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (recvf8 != f8) {
				fprintf(stderr, "expected: %f, got: %f\n", f8, recvf8);
				return 1;
			}

			PGint8 recvi8;
			if (!PQgetf(result, 0, "%int8", 4, &recvi8)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (recvi8 != i8) {
				fprintf(stderr, "expected: %lld, got: %lld\n", i8, recvi8);
				return 1;
			}

			PGtext recvtext;
			if (!PQgetf(result, 0, "%text", 5, &recvtext)) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}
			if (strcmp(recvtext, text)) {
				fprintf(stderr, "expected: %s, got: %s\n", text, recvtext);
				return 1;
			}
	}

	return 0;
}
EOF
gcc -std=c99 -I/usr/include/postgresql -lpq -lpqtypes main.c
./a.out
`
