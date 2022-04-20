// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/comparetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/cyclegraphtest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/entitynodetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
	"github.com/stretchr/testify/require"
)

func TestRel(t *testing.T) {
	for _, s := range []reltest.Suite{
		entitynodetest.Suite,
		cyclegraphtest.Suite,
		comparetest.Suite,
	} {
		t.Run(s.Name, func(t *testing.T) {
			s.Run(t)
		})
	}
}

func TestContradiction(t *testing.T) {
	type A struct{}
	type B struct{}
	schema, err := rel.NewSchema("junk",
		rel.EntityMapping(reflect.TypeOf((*A)(nil))),
		rel.EntityMapping(reflect.TypeOf((*B)(nil))),
	)
	var a, b, typ rel.Var = "a", "b", "typ"
	require.NoError(t, err)
	_, err = rel.NewQuery(schema,
		a.Type((*A)(nil)),
		b.Type((*B)(nil)),
		a.AttrEqVar(rel.Type, typ),
		b.AttrEqVar(rel.Type, typ),
	)
	require.Regexp(t, "failed to construct query: query contains contradiction on Type", err)
}

func TestInvalidData(t *testing.T) {
	type A struct{}
	type B struct{}
	schema := rel.MustSchema("junk",
		rel.EntityMapping(reflect.TypeOf((*A)(nil))),
		rel.EntityMapping(reflect.TypeOf((*B)(nil))),
	)
	t.Run("nil attributes", func(t *testing.T) {
		for _, tc := range []struct {
			value interface{}
			errRE string
		}{
			{struct{}{}, `unknown type handler for struct \{\}`},
			{&struct{}{}, `unknown type handler for \*struct \{\}`},
			{(*A)(nil), `invalid nil \*rel_test.A value`},
		} {
			t.Run(fmt.Sprintf("%T(%v)", tc.value, tc.value), func(t *testing.T) {
				_, err := schema.GetAttribute(rel.Self, tc.value)
				require.Regexp(t, tc.errRE, err)
			})
		}
	})
	t.Run("undefined attribute", func(t *testing.T) {
		_, err := schema.GetAttribute(stringAttr("not-defined"), nil)
		require.EqualError(t, err, `unknown attribute not-defined in schema junk`)
	})
	const invalidClausePrefix = `failed to construct query: failed to process invalid clause `
	t.Run("nil query values", func(t *testing.T) {
		_, err := rel.NewQuery(schema, rel.Var("a").Eq(nil))
		require.Regexp(t, invalidClausePrefix+`\$a = null: invalid nil`, err)

		_, err = rel.NewQuery(schema, rel.Var("a").Eq((*A)(nil)))
		require.Regexp(t, invalidClausePrefix+`\$a = null: invalid nil`, err)
	})
	t.Run("nil entity for attributes", func(t *testing.T) {
		{
			_, err := schema.GetAttribute(rel.Self, nil)
			require.EqualError(t, err, "invalid nil value")
		}
		{
			_, err := schema.GetAttribute(rel.Self, (*A)(nil))
			require.EqualError(t, err, "invalid nil *rel_test.A value")
		}
		{
			require.EqualError(t, schema.IterateAttributes((*A)(nil), func(attribute rel.Attr, value interface{}) error {
				return nil
			}), "invalid nil *rel_test.A value")
		}

	})
	t.Run("bad filters", func(t *testing.T) {
		_, err := rel.NewQuery(schema, rel.Filter("oneArg", "arg")(func() bool {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`oneArg()($arg): invalid func() bool filter `+
				`function for variables [arg] accepts 0 inputs`,
		)

		_, err = rel.NewQuery(schema, rel.Filter("noArgs")(func(a bool) bool {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`noArgs(bool)(): invalid func(bool) bool filter `+
				`function for variables [] accepts 1 inputs`,
		)

		_, err = rel.NewQuery(schema, rel.Filter("badReturn", "arg")(func(a bool) (bool, error) {
			panic("unimplemented")
		}))
		require.EqualError(
			t, err, invalidClausePrefix+
				`badReturn(bool)($arg): invalid non-bool return from `+
				`func(bool) (bool, error) filter function for variables [arg]`,
		)
	})
	t.Run("bad mappings", func(t *testing.T) {
		{
			type T struct{ C chan int }
			_, err := rel.NewSchema("junk",
				rel.EntityMapping(reflect.TypeOf((*T)(nil)),
					rel.EntityAttr(stringAttr("c"), "C"),
				),
			)

			require.EqualError(t, err,
				`failed to construct schema: selector "C" of *rel_test.T has unsupported type chan int`)
		}
	})
	t.Run("bad attributes in database", func(t *testing.T) {
		_, err := rel.NewDatabase(schema, rel.Index{Attrs: []rel.Attr{stringAttr("not-exists")}})
		require.EqualError(t, err, `invalid index attribute: unknown attribute not-exists in schema junk`)
	})
	t.Run("oneOf with more than one value", func(t *testing.T) {
		type oneOf struct {
			A *A
			B *B
		}
		const ab stringAttr = "ab"
		sc := rel.MustSchema("junk",
			rel.AttrType(
				ab, reflect.TypeOf((*interface{})(nil)).Elem(),
			),
			rel.EntityMapping(reflect.TypeOf((*oneOf)(nil)),
				rel.EntityAttr(ab, "A", "B"),
			),
		)
		require.EqualError(t,
			sc.IterateAttributes(&oneOf{A: &A{}, B: &B{}}, func(
				attribute rel.Attr, value interface{},
			) error {
				return nil
			}),
			`*rel_test.oneOf contains second non-nil entry for ab at B`,
		)
	})
}

type stringAttr string

func (sa stringAttr) String() string { return string(sa) }
