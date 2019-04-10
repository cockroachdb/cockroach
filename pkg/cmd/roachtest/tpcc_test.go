// Copyright 2018 The Cockroach Authors.
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
			r := &registry{buildVersion: test.buildVersion}
			if test.expected == expectPanic {
				require.Panics(t, func() {
					w := r.maxSupportedTPCCWarehouses(test.cloud, test.spec)
					t.Errorf("%s %s got unexpected result %d", test.cloud, &test.spec, w)
				})
			} else {
				require.Equal(t, test.expected, r.maxSupportedTPCCWarehouses(test.cloud, test.spec))
			}
		})
	}
}
