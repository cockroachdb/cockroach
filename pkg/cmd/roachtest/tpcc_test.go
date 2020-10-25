// Copyright 2018 The Cockroach Authors.
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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/stretchr/testify/require"
)

func TestTPCCSupportedWarehouses(t *testing.T) {
	const expectPanic = -1
	tests := []struct {
		cloud        string
		spec         clusterSpec
		buildVersion *version.Version
		expected     int
	}{
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`v2.1.0`), 1300},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`v19.1.0-rc.1`), 1250},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`v19.1.0`), 1250},

		{"aws", makeClusterSpec(4, cpu(16)), version.MustParse(`v19.1.0-rc.1`), 2100},
		{"aws", makeClusterSpec(4, cpu(16)), version.MustParse(`v19.1.0`), 2100},

		{"nope", makeClusterSpec(4, cpu(16)), version.MustParse(`v2.1.0`), expectPanic},
		{"gce", makeClusterSpec(5, cpu(160)), version.MustParse(`v2.1.0`), expectPanic},
		{"gce", makeClusterSpec(4, cpu(16)), version.MustParse(`v1.0.0`), expectPanic},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			r := &testRunner{buildVersion: *test.buildVersion}
			if test.expected == expectPanic {
				require.Panics(t, func() {
					w := maxSupportedTPCCWarehouses(r.buildVersion, test.cloud, test.spec)
					t.Errorf("%s %s got unexpected result %d", test.cloud, &test.spec, w)
				})
			} else {
				require.Equal(t, test.expected, maxSupportedTPCCWarehouses(r.buildVersion, test.cloud, test.spec))
			}
		})
	}
}
