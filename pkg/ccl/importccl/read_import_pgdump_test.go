// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package importccl

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestPostgreStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
