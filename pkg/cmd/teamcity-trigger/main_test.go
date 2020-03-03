// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"strings"
	"testing"
)

func TestRunTC(t *testing.T) {
	count := 0
	runTC(func(buildID string, opts map[string]string) {
		count++
		if pkg, ok := opts["env.PKG"]; ok {
			if strings.Contains(pkg, "/vendor/") {
				t.Errorf("unexpected package %s", pkg)
			}
		} else {
			t.Errorf("parameters did not include package: %+v", opts)
		}
	})
	if count == 0 {
		t.Fatal("no builds were created")
	}
}
