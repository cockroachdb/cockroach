// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import "testing"

func TestIsLocalClusterName(t *testing.T) {
	yes := []string{
		"local",
		"local-1",
		"local-foo",
		"local-foo-bar-123-aZy",
	}
	no := []string{
		"loca",
		"locall",
		"local1",
		"local-",
		"local-foo?",
		"local-foo/",
	}

	for _, s := range yes {
		if !IsLocalClusterName(s) {
			t.Errorf("expected '%s' to be a valid local cluster name", s)
		}
	}

	for _, s := range no {
		if IsLocalClusterName(s) {
			t.Errorf("expected '%s' to not be a valid local cluster name", s)
		}
	}
}
