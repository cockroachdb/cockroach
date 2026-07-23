#include <libpq-fe.h>
#include <libpqtypes.h>
#include <limits.h>
#include <string.h>
#include <sys/param.h>

int intervalEqual(PGinterval left, PGinterval right) {
	if (left.years != right.years) {
		return 0;
	}
	if (left.mons != right.mons) {
		return 0;
	}
	if (left.days != right.days) {
		return 0;
	}
	if (left.hours != right.hours) {
		return 0;
	}
	if (left.mins != right.mins) {
		return 0;
	}
	if (left.secs != right.secs) {
		return 0;
	}
	if (left.usecs != right.usecs) {
		return 0;
	}
	return 1;
}

void intervalPrint(PGinterval interval) {
	fprintf(stderr, "years=%d\n", interval.years);
	fprintf(stderr, "mons=%d\n", interval.mons);
	fprintf(stderr, "days=%d\n", interval.days);
	fprintf(stderr, "hours=%d\n", interval.hours);
	fprintf(stderr, "mins=%d\n", interval.mins);
	fprintf(stderr, "secs=%d\n", interval.secs);
	fprintf(stderr, "usecs=%d\n", interval.usecs);
}

int dateEqual(PGdate left, PGdate right) {
	// Only check the fields we explicitly set. The uninitialized fields for our
	// expected result will have non-deterministic values, and those same fields
	// may be populated in the actual result by libpqtypes after decoding the
	// database query's output.
	if (left.isbc != right.isbc) {
		return 0;
	}
	if (left.year != right.year) {
		return 0;
	}
	if (left.mon != right.mon) {
		return 0;
	}
	if (left.mday != right.mday) {
		return 0;
	}
	return 1;
}

void datePrint(PGdate date) {
	fprintf(stderr, "isbc=%d\n", date.isbc);
	fprintf(stderr, "year=%d\n", date.year);
	fprintf(stderr, "mon=%d\n", date.mon);
	fprintf(stderr, "mday=%d\n", date.mday);
}

int timeEqual(PGtime left, PGtime right) {
	// Only check the fields we explicitly set. See comment on dateEqual.
	if (left.hour != right.hour) {
		return 0;
	}
	if (left.min != right.min) {
		return 0;
	}
	if (left.sec != right.sec) {
		return 0;
	}
	if (left.usec != right.usec) {
		return 0;
	}
	if (left.withtz != right.withtz) {
		return 0;
	}
	if (left.gmtoff != right.gmtoff) {
		return 0;
	}
	return 1;
}

void timePrint(PGtime time) {
	fprintf(stderr, "hour=%d\n", time.hour);
	fprintf(stderr, "min=%d\n", time.min);
	fprintf(stderr, "sec=%d\n", time.sec);
	fprintf(stderr, "usec=%d\n", time.usec);
	fprintf(stderr, "withtz=%d\n", time.withtz);
	fprintf(stderr, "gmtoff=%d\n", time.gmtoff);
}

int timestampEqual(PGtimestamp left, PGtimestamp right) {
	if (left.epoch != right.epoch) {
		return 0;
	}
	if (!dateEqual(left.date, right.date)) {
		return 0;
	}
	if (!timeEqual(left.time, right.time)) {
		return 0;
	}
	return 1;
}

void timestampPrint(PGtimestamp ts) {
	fprintf(stderr, "epoch=%lld\n", ts.epoch);
	fprintf(stderr, "date:\n");
	datePrint(ts.date);
	fprintf(stderr, "time:\n");
	timePrint(ts.time);
}

int main(int argc, char *argv[]) {
	if (argc != 2) {
		fprintf(stderr, "usage: %s QUERY\n", argv[0]);
		return 1;
	}
	char *query = argv[1];

	PGconn *conn = PQconnectdb("");
	if (PQstatus(conn) != CONNECTION_OK) {
		fprintf(stderr, "Connection to database failed: %s\n", PQerrorMessage(conn));
		return 1;
	}

	const char *version = PQparameterStatus(conn, "crdb_version");
	if (version == NULL) {
		fprintf(stderr, "ERROR PQparameterStatus: crdb_version not reported: %s\n", PQgeterror());
		return 1;
	}
	if (strncmp(version, "CockroachDB ", strlen("CockroachDB ")) != 0) {
		fprintf(stderr, "crdb_version mismatch: '%s' doesn't start with 'CockroachDB '\n", version);
		return 1;
	}


	/* Always call first on any conn that is to be used with libpqtypes */
	PQinitTypes(conn);

	PGparam *param = PQparamCreate(conn);

	PGbool b = 1;
	if (!PQputf(param, "%bool", b)) {
		fprintf(stderr, "ERROR PQputf(bool): %s\n", PQgeterror());
		return 1;
	}

	char bytes[] = "hello";
	PGbytea bytea;
	bytea.len = sizeof(bytes);
	bytea.data = bytes;
	if (!PQputf(param, "%bytea", &bytea)) {
		fprintf(stderr, "ERROR PQputf(bytea): %s\n", PQgeterror());
		return 1;
	}

	// '1401-01-19'
	PGdate date;
	// TODO(jordan): put this back when #28099 is fixed.
	// date.isbc = 1;
	date.isbc = 0;
	date.year = 1401;
	date.mon  = 0;
	date.mday = 19;
	if (!PQputf(param, "%date", &date)) {
		fprintf(stderr, "ERROR PQputf(date): %s\n", PQgeterror());
		return 1;
	}

	PGnumeric numeric1 = "42";
	if (!PQputf(param, "%numeric", numeric1)) {
		fprintf(stderr, "ERROR PQputf(numeric): %s\n", PQgeterror());
		return 1;
	}

	PGnumeric numeric2 = "-1728718718271827121233.1212121212";
	if (!PQputf(param, "%numeric", numeric2)) {
		fprintf(stderr, "ERROR PQputf(numeric): %s\n", PQgeterror());
		return 1;
	}

	PGfloat8 f8 = 123456.789;
	if (!PQputf(param, "%float8", f8)) {
		fprintf(stderr, "ERROR PQputf(float8): %s\n", PQgeterror());
		return 1;
	}

	PGint8 i8 = INT_MAX;
	if (!PQputf(param, "%int8", i8)) {
		fprintf(stderr, "ERROR PQputf(int8): %s\n", PQgeterror());
		return 1;
	}

	// "20 years 8 months 9 hours 10 mins 15 secs 123456 usecs"
	PGinterval interval;
	interval.years = 20;
	interval.mons  = 8;
	interval.days  = 0; // not used, set to 0
	interval.hours = 9;
	interval.mins  = 10;
	interval.secs  = 15;
	interval.usecs = 123456;
	// TODO(tamird,nvanbenschoten): implement interval binary encoding/decoding.
	if (0) {
		if (!PQputf(param, "%interval", &interval)) {
			fprintf(stderr, "ERROR PQputf(interval): %s\n", PQgeterror());
			return 1;
		}
	}

	PGtext text = "foobar";
	if (!PQputf(param, "%text", text)) {
		fprintf(stderr, "ERROR PQputf(text): %s\n", PQgeterror());
		return 1;
	}

	// '2000-01-19 10:41:06'
	PGtimestamp ts;
	ts.epoch       = 948278466; // expected, but not used in PQputf.
	ts.date.isbc   = 0;
	ts.date.year   = 2000;
	ts.date.mon    = 0;
	ts.date.mday   = 19;
	ts.time.hour   = 10;
	ts.time.min    = 41;
	ts.time.sec    = 6;
	ts.time.usec   = 0;
	ts.time.withtz = 0;
	ts.time.gmtoff = 0; // PQputf normalizes to GMT, so set and expect 0.
	if (!PQputf(param, "%timestamp", &ts)) {
		fprintf(stderr, "ERROR PQputf(timestamp): %s\n", PQgeterror());
		return 1;
	}

	// '2000-01-19 10:41:06-05'
	PGtimestamp tstz;
	tstz.epoch       = 948278466;
	tstz.date.isbc   = 0;
	tstz.date.year   = 2000;
	tstz.date.mon    = 0;
	tstz.date.mday   = 19;
	tstz.time.hour   = 10;
	tstz.time.min    = 41;
	tstz.time.sec    = 6;
	tstz.time.usec   = 0;
	tstz.time.withtz = 1;
	tstz.time.gmtoff = 0;

	if (!PQputf(param, "%timestamptz", &tstz)) {
		fprintf(stderr, "ERROR PQputf(timestamptz): %s\n", PQgeterror());
		return 1;
	}

	char uuidBytes[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
	PGuuid uuid = uuidBytes;
	if (!PQputf(param, "%uuid", uuid)) {
		fprintf(stderr, "ERROR PQputf(uuid): %s\n", PQgeterror());
		return 1;
	}

	PGint8 i;
	PGarray arr;

	arr.ndims = 0;
	arr.param = PQparamCreate(conn);
	int arrLen = 100;
	PGint8 expectedArr[arrLen];

	for (i = 0; i < arrLen; i++) {
		expectedArr[i] = i;
		if (!PQputf(arr.param, "%int8", i)) {
			fprintf(stderr, "ERROR PQputf(arr elem): %s\n", PQgeterror());
			return 1;
		}
	}
	if (!PQputf(param, "%int8[]", &arr)) {
		fprintf(stderr, "ERROR PQputf(arr): %s\n", PQgeterror());
		return 1;
	}

	// resultFormat: 0 for text, 1 for binary.
	for (int resultFormat = 0; resultFormat <= 1; ++resultFormat) {
			PGresult *result = PQparamExec(conn, param, query, resultFormat);
			if(!result) {
				fprintf(stderr, "ERROR: %s\n", PQgeterror());
				return 1;
			}

			int i = 0;

			PGbool recvb;
			if (!PQgetf(result, 0, "%bool", i++, &recvb)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(bool): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (recvb != b) {
				fprintf(stderr, "resultFormat=%d expected: %d, got: %d\n", resultFormat, b, recvb);
				return 1;
			}

			PGbytea recvbytea;
			if (!PQgetf(result, 0, "%bytea", i++, &recvbytea)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(bytea): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (memcmp(recvbytea.data, bytea.data,
				   MIN(recvbytea.len, bytea.len) // lint: uppercase function OK
				) != 0) {
				fprintf(stderr, "resultFormat=%d expected (%d bytes): ", resultFormat, bytea.len);
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

			PGdate recvdate;
			if (!PQgetf(result, 0, "%date", i++, &recvdate)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(date): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (!dateEqual(recvdate, date)) {
				fprintf(stderr, "resultFormat=%d expected:\n", resultFormat);
				datePrint(date);
				fprintf(stderr, "\ngot:\n");
				datePrint(recvdate);
				return 1;
			}

			PGnumeric recvnumeric1;
			if (!PQgetf(result, 0, "%numeric", i++, &recvnumeric1)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(numeric): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (strcmp(recvnumeric1, numeric1)) {
				fprintf(stderr, "resultFormat=%d expected: %s, got: %s\n", resultFormat, numeric1, recvnumeric1);
				return 1;
			}

			PGnumeric recvnumeric2;
			if (!PQgetf(result, 0, "%numeric", i++, &recvnumeric2)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(numeric): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (strcmp(recvnumeric2, numeric2)) {
				fprintf(stderr, "resultFormat=%d expected: %s, got: %s\n", resultFormat, numeric2, recvnumeric2);
				return 1;
			}

			PGfloat8 recvf8;
			if (!PQgetf(result, 0, "%float8", i++, &recvf8)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(float8): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (recvf8 != f8) {
				fprintf(stderr, "resultFormat=%d expected: %f, got: %f\n", resultFormat, f8, recvf8);
				return 1;
			}

			PGint8 recvi8;
			if (!PQgetf(result, 0, "%int8", i++, &recvi8)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(int8): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (recvi8 != i8) {
				fprintf(stderr, "resultFormat=%d expected: %lld, got: %lld\n", resultFormat, i8, recvi8);
				return 1;
			}

			// TODO(tamird,nvanbenschoten): implement interval binary encoding/decoding.
			if (0) {
				PGinterval recvinterval;
				if (!PQgetf(result, 0, "%interval", i++, &recvinterval)) {
					fprintf(stderr, "ERROR resultFormat=%d PQgetf(interval): %s\n", resultFormat, PQgeterror());
					return 1;
				}
				if (!intervalEqual(recvinterval, interval)) {
					fprintf(stderr, "resultFormat=%d expected:\n", resultFormat);
					intervalPrint(interval);
					fprintf(stderr, "\ngot:\n");
					intervalPrint(recvinterval);
					return 1;
				}
			}

			PGtext recvtext;
			if (!PQgetf(result, 0, "%text", i++, &recvtext)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(text): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (strcmp(recvtext, text)) {
				fprintf(stderr, "resultFormat=%d expected: %s, got: %s\n", resultFormat, text, recvtext);
				return 1;
			}

			PGtimestamp recvts;
			if (!PQgetf(result, 0, "%timestamp", i++, &recvts)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(timestamp): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (!timestampEqual(recvts, ts)) {
				fprintf(stderr, "resultFormat=%d expected:\n", resultFormat);
				timestampPrint(ts);
				fprintf(stderr, "\ngot:\n");
				timestampPrint(recvts);
				return 1;
			}

			PGtimestamp recvtstz;
			if (!PQgetf(result, 0, "%timestamptz", i++, &recvtstz)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(timestamptz): %s\n", resultFormat, PQgeterror());
				return 1;
			}

			if (!timestampEqual(recvtstz, tstz)) {
				fprintf(stderr, "resultFormat=%d expected:\n", resultFormat);
				timestampPrint(tstz);
				fprintf(stderr, "\ngot:\n");
				timestampPrint(recvtstz);
				return 1;
			}

			PGuuid recvuuid;
			if (!PQgetf(result, 0, "%uuid", i++, &recvuuid)) {
				fprintf(stderr, "ERROR resultFormat=%d PQgetf(uuid): %s\n", resultFormat, PQgeterror());
				return 1;
			}
			if (strcmp(recvuuid, uuid)) {
				fprintf(stderr, "resultFormat=%d expected: %s, got: %s\n", resultFormat, uuid, recvuuid);
				return 1;
			}

			// Libpqtypes doesn't support text array decoding.
			if (resultFormat == 1) {
				PGarray recvarr;
				if (!PQgetf(result, 0, "%int8[]", i++, &recvarr)) {
					fprintf(stderr, "ERROR resultFormat=%d PQgetf(arr): %s\n", resultFormat, PQgeterror());
					return 1;
				}
				int n = PQntuples(recvarr.res);
				if (arrLen != n) {
					fprintf(stderr, "expected array of size %d, got %d\n", arrLen, n);
					return 1;
				}
				int result[arrLen];
				PGint8 val;
				for (int i = 0; i < arrLen; i++) {
					PQgetf(recvarr.res, i, "%int8", 0, &val);
					if (val != expectedArr[i]) {
						fprintf(stderr, "resultFormat=%d expected %lld at pos %d; got %lld\n", resultFormat, expectedArr[i], i, val);
						return 1;
					}
				}
			}
	}

	return 0;
}
