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

// TestTooManyAttributes tests that errors are returned whenever a predicate
// is constructed with too many attributes. One could imagine disallowing this
// altogether in the schema, but it's somewhat onerous to do so.
func TestTooManyAttributes(t *testing.T) {
	type tooManyAttrs struct {
		F1, F2, F3, F4, F5, F6, F7, F8 *uint32
	}
	sc := rel.MustSchema("too_many",
		rel.EntityMapping(reflect.TypeOf((*tooManyAttrs)(nil)),
			rel.EntityAttr(stringAttr("A1"), "F1"),
			rel.EntityAttr(stringAttr("A2"), "F2"),
			rel.EntityAttr(stringAttr("A3"), "F3"),
			rel.EntityAttr(stringAttr("A4"), "F4"),
			rel.EntityAttr(stringAttr("A5"), "F5"),
			rel.EntityAttr(stringAttr("A6"), "F6"),
			rel.EntityAttr(stringAttr("A7"), "F7"),
			rel.EntityAttr(stringAttr("A8"), "F8"),
		),
	)
	one := uint32(1)
	t.Run("predicate too large", func(t *testing.T) {
		_, err := rel.NewDatabase(sc, rel.Index{
			Where: []rel.IndexWhere{
				{Attr: stringAttr("A1"), Eq: one},
				{Attr: stringAttr("A2"), Eq: one},
				{Attr: stringAttr("A3"), Eq: one},
				{Attr: stringAttr("A4"), Eq: one},
				{Attr: stringAttr("A5"), Eq: one},
				{Attr: stringAttr("A6"), Eq: one},
				{Attr: stringAttr("A7"), Eq: one},
				{Attr: stringAttr("A8"), Eq: one},
				{Attr: rel.Type, Eq: reflect.TypeOf((*tooManyAttrs)(nil))},
			},
		})
		require.EqualError(t, err, `invalid index predicate [{A1 1} {A2 1} {A3 1} {A4 1} {A5 1} {A6 1} {A7 1} {A8 1} {Type *rel_test.tooManyAttrs}] with more than 8 attributes`)
	})
	db, err := rel.NewDatabase(sc, rel.Index{})
	require.NoError(t, err)
	t.Run("index predicate too large", func(t *testing.T) {
		require.Regexp(t, `invalid entity \*rel_test.tooManyAttrs has too many attributes: maximum allowed is 8, have at least \[A1 A2 A3 A4 A5 A6 Self Type A7\]`, db.Insert(&tooManyAttrs{
			F1: &one,
			F2: &one,
			F3: &one,
			F4: &one,
			F5: &one,
			F6: &one,
			F7: &one,
			F8: &one,
		}))

	})
	t.Run("query join predicate too large", func(t *testing.T) {
		require.NoError(t, db.Insert(&tooManyAttrs{
			F1: &one,
			F2: &one,
			F3: &one,
			F4: &one,
		}))
		require.NoError(t, db.Insert(&tooManyAttrs{
			F5: &one,
			F6: &one,
			F7: &one,
			F8: &one,
		}))
		var a, b, c rel.Var = "a", "b", "c"
		base := []rel.Clause{
			a.Type((*tooManyAttrs)(nil)),
			b.Type((*tooManyAttrs)(nil)),
			c.Type((*tooManyAttrs)(nil)),
			rel.Var("f1").Entities(stringAttr("A1"), a, c),
			rel.Var("f2").Entities(stringAttr("A2"), a, c),
			rel.Var("f3").Entities(stringAttr("A3"), a, c),
			rel.Var("f4").Entities(stringAttr("A4"), a, c),
			rel.Var("f5").Entities(stringAttr("A5"), b, c),
			rel.Var("f6").Entities(stringAttr("A6"), b, c),
			rel.Var("f7").Entities(stringAttr("A7"), b, c),
			rel.Var("f8").Entities(stringAttr("A8"), b, c),
		}
		{
			q, err := rel.NewQuery(sc, append(base, c.Type((*tooManyAttrs)(nil)))...)
			require.NoError(t, err)
			require.Regexp(t, "failed to create predicate with more than 8 attributes", q.Iterate(db, func(r rel.Result) error {
				return nil
			}))
		}
		{
			{
				q, err := rel.NewQuery(sc, append(
					base, c.Type((*tooManyAttrs)(nil), (*rel.Schema)(nil)),
				)...)
				require.NoError(t, err)
				require.Regexp(t, "failed to create predicate with more than 8 attributes", q.Iterate(db, func(r rel.Result) error {
					return nil
				}))
			}
		}
	})
}
