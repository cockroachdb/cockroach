// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.

package sessiondata

import (
	"reflect"
	"strings"
	"testing"
)

func TestImpliedSearchPath(t *testing.T) {
	testCases := []struct {
		explicitSearchPath                         []string
		expectedSearchPath                         []string
		expectedSearchPathWithoutImplicitPgCatalog []string
	}{
		{[]string{}, []string{`pg_catalog`}, []string{}},
		{[]string{`pg_catalog`}, []string{`pg_catalog`}, []string{`pg_catalog`}},
		{[]string{`foobar`, `pg_catalog`}, []string{`foobar`, `pg_catalog`}, []string{`foobar`, `pg_catalog`}},
		{[]string{`foobar`}, []string{`pg_catalog`, `foobar`}, []string{`foobar`}},
	}

	for _, tc := range testCases {
		t.Run(strings.Join(tc.explicitSearchPath, ","), func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.Iter()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPath, actualSearchPath) {
				t.Errorf(`Expected search path to be %#v, but was %#v.`, tc.expectedSearchPath, actualSearchPath)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/no-pg-catalog", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.IterWithoutImplicitPGCatalog()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWithoutImplicitPgCatalog, actualSearchPath) {
				t.Errorf(`Expected search path to be %#v, but was %#v.`, tc.expectedSearchPathWithoutImplicitPgCatalog, actualSearchPath)
			}
		})
	}
}
