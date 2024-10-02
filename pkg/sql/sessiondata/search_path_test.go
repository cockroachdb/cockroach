// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessiondata

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests the implied search path when no temporary schema has been created
// by the session.
func TestImpliedSearchPath(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	testCases := []struct {
		explicitSearchPath                                             []string
		expectedSearchPath                                             []string
		expectedSearchPathWithoutImplicitPgSchemas                     []string
		expectedSearchPathWhenTemporarySchemaExists                    []string
		expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists []string
	}{
		{
			explicitSearchPath:                                             []string{},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`, `pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`, `pg_catalog`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`pg_catalog`, `pg_temp`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`pg_catalog`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`pg_temp`, `pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{testTempSchemaName, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`pg_temp`, `pg_temp`, `pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{testTempSchemaName, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`pg_temp`, `pg_catalog`, `pg_catalog`},
			expectedSearchPath:                                             []string{`pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{testTempSchemaName, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`, `pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `foobar`, `pg_catalog`},
			expectedSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`, `pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_catalog`, `pg_catalog`},
			expectedSearchPath:                                             []string{`foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`, `pg_catalog`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `foobar`, `pg_catalog`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, `pg_catalog`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, `foobar`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `foobar`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, `foobar`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `pg_temp`, `pg_temp`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{`pg_catalog`, `foobar`, testTempSchemaName},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`, testTempSchemaName},
		},
		{
			explicitSearchPath:                                             []string{`foobar`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`},
		},
		{
			explicitSearchPath:                                             []string{`foobar`, `foobar`},
			expectedSearchPath:                                             []string{`pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`foobar`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `foobar`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`foobar`},
		},
		{
			explicitSearchPath:                                             []string{`public`},
			expectedSearchPath:                                             []string{`pg_catalog`, `pg_extension`, `public`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`public`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `pg_extension`, `public`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`public`},
		},
		{
			explicitSearchPath:                                             []string{`public`, `public`},
			expectedSearchPath:                                             []string{`pg_catalog`, `pg_extension`, `public`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`public`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `pg_extension`, `public`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`public`},
		},
		{
			explicitSearchPath:                                             []string{`public`, `pg_extension`},
			expectedSearchPath:                                             []string{`pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`public`, `pg_extension`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`public`, `pg_extension`},
		},
		{
			explicitSearchPath:                                             []string{`public`, `public`, `pg_extension`},
			expectedSearchPath:                                             []string{`pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`public`, `pg_extension`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`public`, `pg_extension`},
		},
		{
			explicitSearchPath:                                             []string{`public`, `pg_extension`, `pg_extension`},
			expectedSearchPath:                                             []string{`pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemas:                     []string{`public`, `pg_extension`},
			expectedSearchPathWhenTemporarySchemaExists:                    []string{testTempSchemaName, `pg_catalog`, `public`, `pg_extension`},
			expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists: []string{`public`, `pg_extension`},
		},
	}

	for tcNum, tc := range testCases {
		t.Run(strings.Join(tc.explicitSearchPath, ","), func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.Iter()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPath, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPath,
					actualSearchPath,
				)
			}
			actualSearchPath = make([]string, 0)
			for i, n := 0, searchPath.NumElements(); i < n; i++ {
				actualSearchPath = append(actualSearchPath, searchPath.GetSchema(i))
			}
			if !reflect.DeepEqual(tc.expectedSearchPath, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPath,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/no-pg-schemas", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath)
			actualSearchPath := make([]string, 0)
			iter := searchPath.IterWithoutImplicitPGSchemas()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWithoutImplicitPgSchemas, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWithoutImplicitPgSchemas,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/temp-schema-exists", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath).WithTemporarySchemaName(testTempSchemaName)
			actualSearchPath := make([]string, 0)
			iter := searchPath.Iter()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWhenTemporarySchemaExists, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWhenTemporarySchemaExists,
					actualSearchPath,
				)
			}
		})

		t.Run(strings.Join(tc.explicitSearchPath, ",")+"/no-pg-schemas/temp-schema-exists", func(t *testing.T) {
			searchPath := MakeSearchPath(tc.explicitSearchPath).WithTemporarySchemaName(testTempSchemaName)
			actualSearchPath := make([]string, 0)
			iter := searchPath.IterWithoutImplicitPGSchemas()
			for p, ok := iter.Next(); ok; p, ok = iter.Next() {
				actualSearchPath = append(actualSearchPath, p)
			}
			if !reflect.DeepEqual(tc.expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists, actualSearchPath) {
				t.Errorf(
					`#%d: Expected search path to be %#v, but was %#v.`,
					tcNum,
					tc.expectedSearchPathWithoutImplicitPgSchemasWhenTempSchemaExists,
					actualSearchPath,
				)
			}
		})
	}
}

func TestSearchPathEquals(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	a1 := MakeSearchPath([]string{"x", "y", "z"})
	a2 := MakeSearchPath([]string{"x", "y", "z"})
	assert.True(t, a1.Equals(&a1))
	assert.True(t, a2.Equals(&a2))

	assert.True(t, a1.Equals(&a2))
	assert.True(t, a2.Equals(&a1))

	b := MakeSearchPath([]string{"x", "z", "y"})
	assert.False(t, a1.Equals(&b))

	c1 := MakeSearchPath([]string{"x", "y", "z", "pg_catalog"})
	c2 := MakeSearchPath([]string{"x", "y", "z", "pg_catalog"})
	assert.True(t, c1.Equals(&c2))
	assert.False(t, a1.Equals(&c1))

	d := MakeSearchPath([]string{"x"})
	assert.False(t, a1.Equals(&d))

	e1 := MakeSearchPath([]string{"x", "y", "z"}).WithTemporarySchemaName(testTempSchemaName)
	e2 := MakeSearchPath([]string{"x", "y", "z"}).WithTemporarySchemaName(testTempSchemaName)
	assert.True(t, e1.Equals(&e1))
	assert.True(t, e1.Equals(&e2))
	assert.False(t, e1.Equals(&a1))

	f := MakeSearchPath([]string{"x", "z", "y"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, e1.Equals(&f))

	g := MakeSearchPath([]string{"x", "y", "z", "pg_temp"})
	assert.False(t, e1.Equals(&g))
	assert.False(t, g.Equals(&c1))

	h := MakeSearchPath([]string{"x", "y", "z", "pg_temp"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, g.Equals(&h))

	i := MakeSearchPath([]string{"x", "y", "z", "pg_temp", "pg_catalog"}).WithTemporarySchemaName(testTempSchemaName)
	assert.False(t, i.Equals(&h))
	assert.False(t, i.Equals(&c1))
}

func TestWithTemporarySchema(t *testing.T) {
	testTempSchemaName := `test_temp_schema`

	sp := MakeSearchPath([]string{"x", "y", "z"})
	sp = sp.UpdatePaths([]string{"x", "pg_catalog"})
	assert.True(t, sp.GetTemporarySchemaName() == "")

	sp = sp.WithTemporarySchemaName(testTempSchemaName)
	sp = sp.UpdatePaths([]string{"pg_catalog"})
	assert.True(t, sp.GetTemporarySchemaName() == testTempSchemaName)

	sp = sp.UpdatePaths([]string{"x", "pg_temp"})
	assert.True(t, sp.GetTemporarySchemaName() == testTempSchemaName)
}

func TestSearchPathSpecialChar(t *testing.T) {
	testCases := []struct {
		searchPath               []string
		expectedSearchPathString string
	}{
		{
			searchPath:               []string{`$user`, `public`, `postgis`, `geography`},
			expectedSearchPathString: `"$user", public, postgis, "geography"`,
		},
		{
			searchPath:               []string{`dirt`, `test`, `$user`, `public`, `prod`},
			expectedSearchPathString: `dirt, test, "$user", public, prod`,
		},
		{
			searchPath:               []string{`desc`, `$user`, `$user`, `foo.bar`, `foo.$bar`},
			expectedSearchPathString: `"desc", "$user", "$user", "foo.bar", "foo.$bar"`,
		},
		{
			searchPath:               []string{`bar`, `user`, `limit`, `foo`},
			expectedSearchPathString: `bar, "user", "limit", foo`,
		},
		{
			searchPath:               []string{`bar`, `baz`, `foo`, `foo`},
			expectedSearchPathString: `bar, baz, foo, foo`,
		},
		{
			searchPath:               []string{`$user`, `session_user`, `treat`, `some`},
			expectedSearchPathString: `"$user", "session_user", "treat", "some"`,
		},
		{
			searchPath:               []string{`$user`},
			expectedSearchPathString: `"$user"`,
		},
		{
			searchPath:               []string{`bar`},
			expectedSearchPathString: `bar`,
		},
		{
			searchPath:               []string{`$variadic`, `work`},
			expectedSearchPathString: `"$variadic", "work"`,
		},
	}
	for _, testCase := range testCases {
		t.Run(strings.Join(testCase.searchPath, ", "), func(t *testing.T) {
			sp := MakeSearchPath(testCase.searchPath)
			assert.Equal(t, testCase.expectedSearchPathString, sp.String())
		})
	}
}

func TestRandomSearchPathRoundTrip(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	for i := 0; i < 10000; i++ {
		searchPath := make([]string, 1+rng.Intn(10))
		for j := range searchPath {
			searchPath[j] = randutil.RandString(rng, rng.Intn(10), `ABCabcdef123_-,"\+$€😅`)
		}
		formatted := FormatSearchPaths(searchPath)
		newSearchPath, err := ParseSearchPath(formatted)
		require.NoError(t, err)
		require.Equal(t, searchPath, newSearchPath)
	}
}

func TestParseSearchPathEdgeCases(t *testing.T) {
	testCases := []struct {
		input       string
		expected    []string
		expectedErr bool
	}{
		{input: ``, expected: []string{}},
		{input: `""`, expected: []string{""}},
		{input: `  `, expectedErr: true},
		{input: `a, `, expectedErr: true},
		{input: `,a`, expectedErr: true},
		{input: `a, ,b`, expectedErr: true},
		{input: `a,😇`, expected: []string{"a", "😇"}},
		{input: `a,\abc`, expected: []string{"a", `\abc`}},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			actual, err := ParseSearchPath(tc.input)
			if tc.expectedErr {
				require.ErrorContains(t, err, "invalid value for parameter")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, actual)
			}
		})
	}
}
