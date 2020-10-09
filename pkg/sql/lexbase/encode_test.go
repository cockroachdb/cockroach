// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package lexbase_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
)

func TestEncodeRestrictedSQLIdent(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{`foo`, `foo`},
		{``, `""`},
		{`3`, `"3"`},
		{`foo3`, `foo3`},
		{`foo"`, `"foo"""`},
		{`fo"o"`, `"fo""o"""`},
		{`fOo`, `"fOo"`},
		{`_foo`, `_foo`},
		{`-foo`, `"-foo"`},
		{`select`, `"select"`},
		{`integer`, `"integer"`},
		// N.B. These type names are examples of type names that *should* be
		// unrestricted (left out of the reserved keyword list) because they're not
		// part of the sql standard type name list. This is important for Postgres
		// compatibility. If you find yourself about to change this, don't - you can
		// convince yourself of such by looking at the output of `quote_ident`
		// against a Postgres instance.
		{`int8`, `int8`},
		{`date`, `date`},
		{`inet`, `inet`},
	}

	for _, tc := range testCases {
		var buf bytes.Buffer
		lexbase.EncodeRestrictedSQLIdent(&buf, tc.input, lexbase.EncBareStrings)
		out := buf.String()

		if out != tc.output {
			t.Errorf("`%s`: expected `%s`, got `%s`", tc.input, tc.output, out)
		}
	}
}
