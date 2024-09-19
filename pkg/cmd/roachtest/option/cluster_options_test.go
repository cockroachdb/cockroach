// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package option

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestApply(t *testing.T) {
	type validContainer struct {
		VirtualClusterOptions
		ConnectionOptions map[string]string
	}

	type wrongTypeContainer struct {
		DBName int
	}

	type invalidOptionContainer struct {
		DBName        string
		InvalidOption string
	}

	testCases := []struct {
		name          string
		optionsStruct string
		options       []OptionFunc
		expected      any
		expectedErr   string
	}{
		{
			name:          "no custom option",
			optionsStruct: "valid_container",
			expected:      validContainer{},
		},
		{
			name:          "setting a custom virtual cluster name",
			optionsStruct: "valid_container",
			options:       []OptionFunc{VirtualClusterName("app")},
			expected: validContainer{
				VirtualClusterOptions: VirtualClusterOptions{VirtualClusterName: "app"},
			},
		},
		{
			name:          "setting a single connection option",
			optionsStruct: "valid_container",
			options:       []OptionFunc{ConnectionOption("name", "val")},
			expected:      validContainer{ConnectionOptions: map[string]string{"name": "val"}},
		},
		{
			name:          "setting multiple connection options",
			optionsStruct: "valid_container",
			options:       []OptionFunc{ConnectionOption("name", "val"), ConnectionOption("name2", "val2")},
			expected:      validContainer{ConnectionOptions: map[string]string{"name": "val", "name2": "val2"}},
		},
		{
			name:          "setting a connection option and a virtual cluster",
			optionsStruct: "valid_container",
			options:       []OptionFunc{ConnectionOption("name", "val"), VirtualClusterName("app")},
			expected: validContainer{
				VirtualClusterOptions: VirtualClusterOptions{VirtualClusterName: "app"},
				ConnectionOptions:     map[string]string{"name": "val"},
			},
		},
		{
			name:          "using VirtualClusterOptions directly",
			optionsStruct: "virtual_cluster_options",
			options:       []OptionFunc{VirtualClusterName("app")},
			expected:      VirtualClusterOptions{VirtualClusterName: "app"},
		},
		{
			name:          "setting a non-applicable option",
			optionsStruct: "valid_container",
			options:       []OptionFunc{User("user")},
			expectedErr:   `non-applicable option "User" for *option.validContainer`,
		},
		{
			name:          "using a wrong type for a supported option",
			optionsStruct: "wrong_type",
			options:       []OptionFunc{DBName("hello")},
			expectedErr:   `failed to set "DBName" on *option.wrongTypeContainer: reflect.Set: value of type string is not assignable to type int`,
		},
		{
			name:          "using an unsupported option",
			optionsStruct: "invalid_option",
			options:       []OptionFunc{DBName("mydb")},
			expectedErr:   `option.invalidOptionContainer has unknown option "InvalidOption"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var err error
			var container any
			switch tc.optionsStruct {
			case "valid_container":
				var c validContainer
				err = Apply(&c, tc.options...)
				container = c
			case "virtual_cluster_options":
				var c VirtualClusterOptions
				err = Apply(&c, tc.options...)
				container = c
			case "wrong_type":
				var c wrongTypeContainer
				err = Apply(&c, tc.options...)
				container = c
			case "invalid_option":
				var c invalidOptionContainer
				err = Apply(&c, tc.options...)
				container = c
			default:
				t.Fatalf("invalid optionsStruct %s", tc.optionsStruct)
			}

			if tc.expectedErr == "" {
				require.NoError(t, err)
				require.Equal(t, tc.expected, container)
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
			require.NoError(t, Apply(&opts, o))
			require.Equal(t, d.o, opts.ConnectionOptions["connect_timeout"])
		})
	}
}
