// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestPostgreStream(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const sql = `
select 1;
-- select 2;
select 3;
select 4;
select 5;
select '12345678901234567890123456789012345678901234567890123456789012345678901234567890';
--
`

	p := newPostgreStream(strings.NewReader(sql), defaultScanBuffer)
	var sb strings.Builder
	for {
		s, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%s;\n", s)
	}
	const expect = `SELECT 1;
SELECT 3;
SELECT 4;
SELECT 5;
SELECT '12345678901234567890123456789012345678901234567890123456789012345678901234567890';
`
	got := sb.String()
	if expect != got {
		t.Fatalf("got %q, expected %q", got, expect)
	}
}

func TestPostgreStreamCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const sql = `
CREATE TABLE public.second (
    i int8 NOT NULL,
    s text
);


--
-- Data for Name: second; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.second (i, s) FROM stdin;
0	0
1	1
2	2
3	3
4	4
5	5
6	6
\.


--
-- Name: second second_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.second
    ADD CONSTRAINT second_pkey PRIMARY KEY (i);

--
-- Name: t; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.t (
    s text
);


--
-- Data for Name: t; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.t (s) FROM stdin;

\\.
;
\.


--
-- PostgreSQL database dump complete
--
`

	p := newPostgreStream(strings.NewReader(sql), defaultScanBuffer)
	var sb strings.Builder
	for {
		s, err := p.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%s;\n", s)
	}
	const expect = `CREATE TABLE public.second (i INT8 NOT NULL, s STRING);
COPY public.second (i, s) FROM STDIN;
"0"	"0";
"1"	"1";
"2"	"2";
"3"	"3";
"4"	"4";
"5"	"5";
"6"	"6";
COPY done;
ALTER TABLE public.second ADD CONSTRAINT second_pkey PRIMARY KEY (i);
CREATE TABLE public.t (s STRING);
COPY public.t (s) FROM STDIN;
"";
"\\.";
";";
COPY done;
`
	got := sb.String()
	if expect != got {
		t.Fatalf("got %s, expected %s", got, expect)
	}
}

func TestImportCreateSequenceAs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	var data string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			_, _ = w.Write([]byte(data))
		}
	}))
	defer srv.Close()

	tests := []struct {
		name        string
		data        string
		verifyQuery string
		err         string
		expected    [][]string
	}{
		{
			name: "as integer",
			data: `
				CREATE SEQUENCE public.a_seq AS integer
				START WITH 2
				INCREMENT BY 1
				MINVALUE 0
				MAXVALUE 234567
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 0 MAXVALUE 234567 INCREMENT 1 START 2`,
			}},
		},
		{
			name: "as integer desc",
			data: `
				CREATE SEQUENCE public.a_seq AS integer
				START WITH -20000 
				INCREMENT BY -1
				MINVALUE -20000
				MAXVALUE 0
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE -20000 MAXVALUE 0 INCREMENT -1 START -20000`,
			}},
		},
		{
			name: "as bigint",
			data: `
				CREATE SEQUENCE public.a_seq AS bigint
				START WITH 1
				INCREMENT BY 1
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT 1 START 1`,
			}},
		},
		{
			name: "as smallint",
			data: `
				CREATE SEQUENCE public.a_seq AS smallint
				START WITH 2
				INCREMENT BY 3
				CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int2 MINVALUE 1 MAXVALUE 32767 INCREMENT 3 START 2`,
			}},
		},
		{
			name: `MAXINT overrides integer type default max`,
			data: `CREATE SEQUENCE public.a_seq
						AS integer
						START WITH 1
						INCREMENT BY 1
						MAXVALUE 9001
						CACHE 1;`,
			verifyQuery: `SHOW CREATE SEQUENCE a_seq`,
			expected: [][]string{{
				"a_seq", `CREATE SEQUENCE public.a_seq AS int MINVALUE 1 MAXVALUE 9001 INCREMENT 1 START 1`,
			}},
		},
	}
	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {
			// Set up clean testing enviornment.
			sqlDB.Exec(t, `DROP SEQUENCE IF EXISTS a_seq`)

			importDumpQuery := fmt.Sprintf(`IMPORT PGDUMP ($1)`)
			data = test.data

			if test.err != "" {
				sqlDB.ExpectErr(t, test.err, importDumpQuery, srv.URL)
				sqlDB.ExpectErr(t, `relation "a_seq" does not exist`, `DROP SEQUENCE a_seq`)

			} else {
				// Import PGDump and verify expected behaviour.
				sqlDB.Exec(t, importDumpQuery, srv.URL)
				sqlDB.CheckQueryResults(t, test.verifyQuery, test.expected)
				sqlDB.Exec(t, `DROP SEQUENCE a_seq`)
			}
		})
	}
}
