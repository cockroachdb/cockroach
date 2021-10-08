// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clusterversion

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"
)

func TestVersionsAreValid(t *testing.T) {
	defer leaktest.AfterTest(t)()

	require.NoError(t, versionsSingleton.Validate())
}

func TestVersionFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := ClusterVersion{
		Version: roachpb.Version{
			Major: 1,
			Minor: 2,
			Patch: 3,
		},
	}

	if actual, expected := string(redact.Sprint(v.Version)), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}

	if actual, expected := v.Version.String(), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}

	if actual, expected := v.String(), `1.2`; actual != expected {
		t.Errorf("expected %q, got %q", expected, actual)
	}
}

func TestClusterVersionPrettyPrint(t *testing.T) {
	defer leaktest.AfterTest(t)()

	cv := func(major, minor, patch, internal int32) ClusterVersion {
		return ClusterVersion{
			Version: roachpb.Version{
				Major:    major,
				Minor:    minor,
				Patch:    patch,
				Internal: internal,
			},
		}
	}

	var tests = []struct {
		cv  ClusterVersion
		exp string
	}{
		{cv(19, 2, 1, 5), "19.2-5"},
		{cv(20, 1, 0, 4), "20.1-4"},
		{cv(20, 2, 0, 7), "20.2-7(fence)"},
		{cv(20, 2, 0, 4), "20.2-4"},
		{cv(20, 2, 1, 5), "20.2-5(fence)"},
		{cv(20, 2, 1, 4), "20.2-4"},
	}
	for _, test := range tests {
		if actual := test.cv.PrettyPrint(); actual != test.exp {
			t.Errorf("expected %s, got %q", test.exp, actual)
		}
	}
}

func TestGetVersionsBetween(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Define a list of versions v3..v9
	var vs keyedVersions
	for i := 3; i < 10; i++ {
		vs = append(vs, keyedVersion{
			Key:     Key(42),
			Version: roachpb.Version{Major: int32(i)},
		})
	}
	cv := func(major int32) ClusterVersion {
		return ClusterVersion{Version: roachpb.Version{Major: major}}
	}
	list := func(first, last int32) []ClusterVersion {
		var cvs []ClusterVersion
		for i := first; i <= last; i++ {
			cvs = append(cvs, cv(i))
		}
		return cvs
	}

	var tests = []struct {
		from, to ClusterVersion
		exp      []ClusterVersion
	}{
		{cv(5), cv(8), list(6, 8)},
		{cv(1), cv(1), []ClusterVersion{}},
		{cv(7), cv(7), []ClusterVersion{}},
		{cv(1), cv(5), list(3, 5)},
		{cv(6), cv(12), list(7, 9)},
		{cv(4), cv(5), list(5, 5)},
	}

	for _, test := range tests {
		actual := listBetweenInternal(test.from, test.to, vs)
		if len(actual) != len(test.exp) {
			t.Errorf("expected %d versions, got %d", len(test.exp), len(actual))
		}

		for i := range test.exp {
			if actual[i] != test.exp[i] {
				t.Errorf("%s version incorrect: expected %s, got %s", humanize.Ordinal(i), test.exp[i], actual[i])
			}
		}
	}
}
