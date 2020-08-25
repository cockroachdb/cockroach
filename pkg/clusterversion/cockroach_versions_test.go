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
