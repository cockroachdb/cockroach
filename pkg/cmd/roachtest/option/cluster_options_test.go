// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApply(t *testing.T) {
	type container struct {
		VirtualClusterName string
		ConnectionOption   map[string]string
	}

	testCases := []struct {
		name        string
		options     []CustomOption
		expected    container
		expectedErr string
	}{
		{
			name:     "no custom option",
			expected: container{},
		},
		{
			name:     "setting a custom virtual cluster name",
			options:  []CustomOption{VirtualClusterName("app")},
			expected: container{VirtualClusterName: "app"},
		},
		{
			name:     "setting a single connection option",
			options:  []CustomOption{ConnectionOption("name", "val")},
			expected: container{ConnectionOption: map[string]string{"name": "val"}},
		},
		{
			name:     "setting multiple connection options",
			options:  []CustomOption{ConnectionOption("name", "val"), ConnectionOption("name2", "val2")},
			expected: container{ConnectionOption: map[string]string{"name": "val", "name2": "val2"}},
		},
		{
			name:     "setting a connection option and a virtual cluster",
			options:  []CustomOption{ConnectionOption("name", "val"), VirtualClusterName("app")},
			expected: container{VirtualClusterName: "app", ConnectionOption: map[string]string{"name": "val"}},
		},
		{
			name: "using a wrong type for a supported option",
			// Should not be possible using the public API, but we verify
			// how this function behaves in this case anyway.
			options:     []CustomOption{{name: "VirtualClusterName", apply: overwrite(10)}},
			expectedErr: `failed to set "VirtualClusterName"`,
		},
		{
			name:        "using an unsupported option",
			options:     []CustomOption{DBName("mydb")},
			expectedErr: "invalid option DBName for *option.container",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var c container
			err := Apply(&c, tc.options)

			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, c)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			}
		})
	}
}

func TestTimeoutCalculation(t *testing.T) {
	var opts ConnOptions
	for _, d := range []struct {
		t time.Duration
		o string
	}{
		{
			t: time.Second,
			o: "1",
		},
		{
			t: time.Millisecond,
			o: "1",
		},
		{
			t: time.Minute,
			o: "60",
		},
	} {
		t.Run(d.t.String(), func(t *testing.T) {
			o := ConnectTimeout(d.t)
			require.NoError(t, Apply(&opts, []CustomOption{o}))
			require.Equal(t, d.o, opts.ConnectionOption["connect_timeout"])
		})
	}
}
