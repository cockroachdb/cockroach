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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"flag"
	"go/build"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/testutils/buildutil"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestStdFlagToPflag(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		if strings.HasPrefix(f.Name, "test.") {
			return
		}
		if pf := cf.Lookup(f.Name); pf == nil {
			t.Errorf("unable to find \"%s\"", f.Name)
		}
	})
}

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)()
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	imports, err := buildutil.TransitiveImports("github.com/cockroachdb/cockroach", true)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{
		"testing",  // defines flags
		"go/build", // probably not something we want in the main binary
		"github.com/cockroachdb/cockroach/security/securitytest", // contains certificates
	} {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("The cockroach binary includes %s, which is forbidden", forbidden)
		}
	}
}

func TestCacheFlagValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f := startCmd.Flags()
	args := []string{"--cache", "100MB"}
	if err := f.Parse(args); err != nil {
		t.Fatal(err)
	}

	ctx := cliContext
	const expectedCacheSize = 100 * 1000 * 1000
	if expectedCacheSize != ctx.CacheSize {
		t.Errorf("expected %d, but got %d", expectedCacheSize, ctx.CacheSize)
	}
}
