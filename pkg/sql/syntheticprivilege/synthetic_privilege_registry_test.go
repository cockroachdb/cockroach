// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syntheticprivilege

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsePrivilegePath(t *testing.T) {
	for _, tc := range []struct {
		regex        string
		expectedType Object
		error        string
	}{
		{
			regex: "/garbageasdf/",
			error: "no prefix match found for privilege path /garbageasdf/",
		},
		{
			regex:        "/global/",
			expectedType: GlobalPrivilegeObject,
		},
		{
			regex: "/global/unexpected",
			error: "/global/unexpected does not match regex pattern (/global/)$",
		},
	} {
		actualType, err := Parse(tc.regex)
		if tc.error != "" {
			if err == nil {
				t.Fatalf("expected error %s, no error was given", tc.error)
			}
			require.Equal(t, tc.error, err.Error())
		} else {
			require.NoError(t, err)
			require.Equal(t, reflect.TypeOf(actualType), reflect.TypeOf(tc.expectedType))
			require.Equal(t, actualType.GetPath(), tc.expectedType.GetPath())
		}
	}
}
