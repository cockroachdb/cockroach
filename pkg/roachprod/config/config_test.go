// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
