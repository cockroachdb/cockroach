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
// Author: Tamir Duberstein (tamird@gmail.com)

package driver

import (
	"go/build"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestNoLinkForbidden(t *testing.T) {
	defer leaktest.AfterTest(t)
	if build.Default.GOPATH == "" {
		t.Skip("GOPATH isn't set")
	}

	// TODO(pmattis): Pass false instead of true for the cgo parameter once
	// rpc/codec can be compiled without the c-lz4 and c-snappy dependencies.
	imports, err := testutils.TransitiveImports("github.com/cockroachdb/cockroach/sql/driver", true)
	if err != nil {
		t.Fatal(err)
	}

	for _, forbidden := range []string{
		"C",       // cross compilation
		"testing", // defines flags
		"github.com/cockroachdb/cockroach/util/log", // defines flags
	} {
		if _, ok := imports[forbidden]; ok {
			t.Errorf("sql/driver includes %s, which is forbidden", forbidden)
		}
	}
}
