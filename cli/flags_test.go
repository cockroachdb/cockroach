// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"flag"
	"go/build"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestStdFlagToPflag(t *testing.T) {
	defer leaktest.AfterTest(t)
	cf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") {
			return
		}
		n := normalizeStdFlagName(f.Name)
		if pf := cf.Lookup(n); pf == nil {
			t.Errorf("unable to find \"%s\"", n)
		}
	})
}

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	imports, err := testutils.TransitiveImports("github.com/cockroachdb/cockroach", true)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{
		"testing", // defines flags
		"github.com/cockroachdb/cockroach/security/securitytest", // contains certificates
		"github.com/cockroachdb/cockroach/testutils",
	} {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("The cockroach binary includes %s, which is forbidden", forbidden)
		}
	}
}
