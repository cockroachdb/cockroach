// Copyright 2016 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package timeutil

import (
	"testing"
	"time"
)

func TestTimeConversion(t *testing.T) {
	tests := []struct {
		start     string
		format    string
		tm        string
		revformat string
		reverse   string
	}{
		{`2006-10-12`, `%Y-%m-%d`, `2006-10-12T00:00:00Z`, ``, ``},
		{`10/12/06`, `%m/%d/%y`, `2006-10-12T00:00:00Z`, ``, ``},
		{`20061012 01:03:02`, `%Y%m%d %H:%S:%M`, `2006-10-12T01:02:03Z`, ``, ``},
		{`2018 10 4`, `%Y %W %w`, `2018-03-08T00:00:00Z`, ``, ``},
		{`2018 10 4`, `%Y %U %w`, `2018-03-15T00:00:00Z`, ``, ``},
		// On BSD variants (OS X, FreeBSD), %p cannot be parsed before
		// hour specifiers, so be sure that they appear in this order.
		{`20161012 11 PM`, `%Y%m%d %I %p`, `2016-10-12T23:00:00Z`, ``, ``},
		{`Wed Oct 05 2016`, `%a %b %d %Y`, `2016-10-05T00:00:00Z`, ``, ``},
		{`Wednesday October 05 2016`, `%A %B %d %Y`, `2016-10-05T00:00:00Z`, ``, ``},
		// %j and %z cannot be used reliably to parse a date from text (%z
		// is non-standard; %j does not work on some platforms, including
		// OSX); so test it only for rendering.
		{`2016-10-12 010203`, `%Y-%m-%d %H%M%S`, `2016-10-12T01:02:03Z`, `%j %z`, `286 +0000`},
	}

	for _, test := range tests {
		tm, err := Strptime(test.start, test.format)
		if err != nil {
			t.Errorf("strptime(%q, %q): %v", test.start, test.format, err)
			continue
		}
		tm = tm.UTC()

		tmS := tm.Format(time.RFC3339Nano)
		if tmS != test.tm {
			t.Errorf("strptime(%q, %q): got %q, expected %q", test.start, test.format, tmS, test.tm)
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

		revS, err := Strftime(tm, revfmt)
		if err != nil {
			t.Errorf("strftime(%q, %q): %v", tm, revfmt, err)
			continue
		}
		if ref != revS {
			t.Errorf("strftime(%q, %q): got %q, expected %q", tm, revfmt, revS, ref)
		}
	}
}
