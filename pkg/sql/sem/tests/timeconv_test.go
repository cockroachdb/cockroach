// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tests

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestTimeConversion(t *testing.T) {
	tests := []struct {
		start     string
		format    string
		tm        string
		revformat string
		reverse   string
	}{
		// %a %A %b %B (+ %Y)
		{`Wed Oct 05 2016`, `%a %b %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		{`Wednesday October 05 2016`, `%A %B %d %Y`, `2016-10-05 00:00:00+00:00`, ``, ``},
		// %c
		{`Wed Oct 5 01:02:03 2016`, `%c`, `2016-10-05 01:02:03+00:00`, ``, ``},
		// %C %d (+ %m %y)
		{`20 06 10 12`, `%C %y %m %d`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %D
		{`10/12/06`, `%D`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %e (+ %Y %m)
		{`2006 10  3`, `%Y %m %e`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %f (+ %c)
		{`Wed Oct 5 01:02:03 2016 .123`, `%c .%f`, `2016-10-05 01:02:03.123+00:00`, `.%f`, `.123000000`},
		{`Wed Oct 5 01:02:03 2016 .123456`, `%c .%f`, `2016-10-05 01:02:03.123456+00:00`, `.%f`, `.123456000`},
		{`Wed Oct 5 01:02:03 2016 .123456789`, `%c .%f`, `2016-10-05 01:02:03.123457+00:00`, `.%f`, `.123457000`},
		{`Wed Oct 5 01:02:03 2016 .999999999`, `%c .%f`, `2016-10-05 01:02:04+00:00`, `.%f`, `.000000000`},
		// %F
		{`2006-10-03`, `%F`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %h (+ %Y %d)
		{`2006 Oct 03`, `%Y %h %d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %H (+ %S %M)
		{`20061012 01:03:02`, `%Y%m%d %H:%S:%M`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %I (+ %Y %m %d)
		{`20161012 11`, `%Y%m%d %I`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %j (+ %Y)
		{`2016 286`, `%Y %j`, `2016-10-12 00:00:00+00:00`, ``, ``},
		// %k (+ %Y %m %d)
		{`20061012 23`, `%Y%m%d %k`, `2006-10-12 23:00:00+00:00`, ``, ``},
		// %l (+ %Y %m %d %p)
		{`20061012  5 PM`, `%Y%m%d %l %p`, `2006-10-12 17:00:00+00:00`, ``, ``},
		// %n (+ %Y %m %d)
		{"2006\n10\n03", `%Y%n%m%n%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %p cannot be parsed before hour specifiers, so be sure that
		// they appear in this order.
		{`20161012 11 PM`, `%Y%m%d %I %p`, `2016-10-12 23:00:00+00:00`, ``, ``},
		{`20161012 11 AM`, `%Y%m%d %I %p`, `2016-10-12 11:00:00+00:00`, ``, ``},
		// %r
		{`20161012 11:02:03 PM`, `%Y%m%d %r`, `2016-10-12 23:02:03+00:00`, ``, ``},
		// %R
		{`20161012 11:02`, `%Y%m%d %R`, `2016-10-12 11:02:00+00:00`, ``, ``},
		// %s
		{`1491920586`, `%s`, `2017-04-11 14:23:06+00:00`, ``, ``},
		// %t (+ %Y %m %d)
		{"2006\t10\t03", `%Y%t%m%t%d`, `2006-10-03 00:00:00+00:00`, ``, ``},
		// %T (+ %Y %m %d)
		{`20061012 01:02:03`, `%Y%m%d %T`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %U %u (+ %Y)
		{`2018 10 4`, `%Y %U %u`, `2018-03-15 00:00:00+00:00`, ``, ``},
		// %W %w (+ %Y)
		{`2018 10 4`, `%Y %W %w`, `2018-03-08 00:00:00+00:00`, ``, ``},
		// %x
		{`10/12/06`, `%x`, `2006-10-12 00:00:00+00:00`, ``, ``},
		// %X
		{`20061012 01:02:03`, `%Y%m%d %X`, `2006-10-12 01:02:03+00:00`, ``, ``},
		// %y (+ %m %d)
		{`000101`, `%y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`680101`, `%y%m%d`, `2068-01-01 00:00:00+00:00`, ``, ``},
		{`690101`, `%y%m%d`, `1969-01-01 00:00:00+00:00`, ``, ``},
		{`990101`, `%y%m%d`, `1999-01-01 00:00:00+00:00`, ``, ``},
		// %Y
		{`19000101`, `%Y%m%d`, `1900-01-01 00:00:00+00:00`, ``, ``},
		{`20000101`, `%Y%m%d`, `2000-01-01 00:00:00+00:00`, ``, ``},
		{`30000101`, `%Y%m%d`, `3000-01-01 00:00:00+00:00`, ``, ``},
		// %z causes the time zone to adjust the time when parsing, but the time zone information
		// is not retained when printing the timestamp out back.
		{`20160101 13:00 +0655`, `%Y%m%d %H:%M %z`, `2016-01-01 06:05:00+00:00`, `%Y%m%d %H:%M %z`, `20160101 06:05 +0000`},
	}

	for _, test := range tests {
		ctx := parser.NewTestingEvalContext()
		defer ctx.Mon.Stop(context.Background())
		exprStr := fmt.Sprintf("experimental_strptime('%s', '%s')", test.start, test.format)
		expr, err := parser.ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err := expr.TypeCheck(nil, types.Timestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err := typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		ts, ok := r.(*parser.DTimestampTZ)
		if !ok {
			t.Errorf("%s: result not a timestamp: %s", exprStr, r)
			continue
		}

		tmS := ts.String()
		tmS = tmS[1 : len(tmS)-1] // strip the quote delimiters
		if tmS != test.tm {
			t.Errorf("%s: got %q, expected %q", exprStr, tmS, test.tm)
			continue
		}

		revfmt := test.format
		if test.revformat != "" {
			revfmt = test.revformat
		}

		ref := test.start
		if test.reverse != "" {
			ref = test.reverse
		}

		exprStr = fmt.Sprintf("experimental_strftime('%s'::timestamp, '%s')", tmS, revfmt)
		expr, err = parser.ParseExpr(exprStr)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		typedExpr, err = expr.TypeCheck(nil, types.Timestamp)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		r, err = typedExpr.Eval(ctx)
		if err != nil {
			t.Errorf("%s: %v", exprStr, err)
			continue
		}
		rs, ok := r.(*parser.DString)
		if !ok {
			t.Errorf("%s: result not a string: %s", exprStr, r)
			continue
		}
		revS := string(*rs)
		if ref != revS {
			t.Errorf("%s: got %q, expected %q", exprStr, revS, ref)
		}
	}
}
