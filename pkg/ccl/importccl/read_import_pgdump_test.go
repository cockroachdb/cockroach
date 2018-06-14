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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
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
`

	p := newPostgreStream(strings.NewReader(sql), 1, 20)
	var sb strings.Builder
	for i := 0; i < 4; i++ {
		s, err := p.Next()
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%s;\n", s)
	}
	const expect = `SELECT 1;
SELECT 3;
SELECT 4;
SELECT 5;
`
	got := sb.String()
	if expect != got {
		t.Fatalf("got %q, expected %q", got, expect)
	}

	if s, err := p.Next(); !testutils.IsError(err, "buffer too small: 32") {
		t.Fatalf("unexpected: %v (%v)", err, s)
	}
}
