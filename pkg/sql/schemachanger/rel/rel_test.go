// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/comparetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/cyclegraphtest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/internal/entitynodetest"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel/reltest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

// TestTooManyAttributesInSchema tests that a schema with too many attributes
// causes an error.
func TestTooManyAttributesInSchema(t *testing.T) {
	// At time of writing, there are two system attributes. That leaves 30
	// bits of the 32-bit ordinal set for use by the user.
	type tooManyAttrs struct {
		F0, F1, F2, F3, F4, F5, F6, F7, F8, F9           *uint32
		F10, F11, F12, F13, F14, F15, F16, F17, F18, F19 *uint32
		F20, F21, F22, F23, F24, F25, F26, F27, F28, F29 *uint32
		F30                                              *uint32
	}
	justEnoughMappings := []rel.EntityMappingOption{
		rel.EntityAttr(stringAttr("A0"), "F0"),
		rel.EntityAttr(stringAttr("A1"), "F1"),
		rel.EntityAttr(stringAttr("A2"), "F2"),
		rel.EntityAttr(stringAttr("A3"), "F3"),
		rel.EntityAttr(stringAttr("A4"), "F4"),
		rel.EntityAttr(stringAttr("A5"), "F5"),
		rel.EntityAttr(stringAttr("A6"), "F6"),
		rel.EntityAttr(stringAttr("A7"), "F7"),
		rel.EntityAttr(stringAttr("A8"), "F8"),
		rel.EntityAttr(stringAttr("A9"), "F9"),
		rel.EntityAttr(stringAttr("A10"), "F10"),
		rel.EntityAttr(stringAttr("A11"), "F11"),
		rel.EntityAttr(stringAttr("A12"), "F12"),
		rel.EntityAttr(stringAttr("A13"), "F13"),
		rel.EntityAttr(stringAttr("A14"), "F14"),
		rel.EntityAttr(stringAttr("A15"), "F15"),
		rel.EntityAttr(stringAttr("A16"), "F16"),
		rel.EntityAttr(stringAttr("A17"), "F17"),
		rel.EntityAttr(stringAttr("A18"), "F18"),
		rel.EntityAttr(stringAttr("A19"), "F19"),
		rel.EntityAttr(stringAttr("A20"), "F20"),
		rel.EntityAttr(stringAttr("A21"), "F21"),
		rel.EntityAttr(stringAttr("A22"), "F22"),
		rel.EntityAttr(stringAttr("A23"), "F23"),
		rel.EntityAttr(stringAttr("A24"), "F24"),
		rel.EntityAttr(stringAttr("A25"), "F25"),
		rel.EntityAttr(stringAttr("A26"), "F26"),
		rel.EntityAttr(stringAttr("A27"), "F27"),
		rel.EntityAttr(stringAttr("A28"), "F28"),
		rel.EntityAttr(stringAttr("A29"), "F29"),
	}
	{
		_, err := rel.NewSchema("just_enough",
			rel.EntityMapping(reflect.TypeOf((*tooManyAttrs)(nil)), justEnoughMappings...),
		)
		require.NoError(t, err)
	}
	{
		_, err := rel.NewSchema("too_many",
			rel.EntityMapping(reflect.TypeOf((*tooManyAttrs)(nil)),
				append(justEnoughMappings, rel.EntityAttr(stringAttr("A30"), "F30"))...,
			),
		)
		require.Regexp(t, "too many attributes", err)
	}
}

type stringAttr string

func (sa stringAttr) String() string { return string(sa) }

// TestTooManyAttributesInValues tests that errors are returned whenever a
// predicate is constructed with too many attributes. One could imagine
// disallowing this altogether in the schema, but it's somewhat onerous to do
// so.
func TestTooManyAttributesInValues(t *testing.T) {
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
			require.Regexp(t, "failed to create predicate with more than 8 attributes", q.Iterate(db, &rel.QueryStats{}, func(r rel.Result) error {
				return nil
			}))
		}
		{
			{
				q, err := rel.NewQuery(sc, append(
					base, c.Type((*tooManyAttrs)(nil), (*rel.Schema)(nil)),
				)...)
				require.NoError(t, err)
				require.Regexp(t, "failed to create predicate with more than 8 attributes", q.Iterate(db, nil, func(r rel.Result) error {
					return nil
				}))
			}
		}
	})
}

func TestRuleValidation(t *testing.T) {
	type entity struct {
		F1, F2 *uint32
	}
	a1, a2 := stringAttr("a1"), stringAttr("a2")
	sc := rel.MustSchema("rules",
		rel.EntityMapping(reflect.TypeOf((*entity)(nil)),
			rel.EntityAttr(a1, "F1"),
			rel.EntityAttr(a2, "F2"),
		),
	)
	require.PanicsWithError(t, "invalid rule test: [a b] input variable are not used", func() {
		sc.Def2("test", "a", "b", func(a, b rel.Var) rel.Clauses {
			return rel.Clauses{}
		})
	})
	require.PanicsWithError(t, "invalid rule test: [b] input variable are not used", func() {
		sc.Def2("test", "a", "b", func(a, b rel.Var) rel.Clauses {
			return rel.Clauses{a.AttrEqVar(rel.Self, a)}
		})
	})
	require.PanicsWithError(t, "invalid rule test: [c] are not defined variables", func() {
		sc.Def2("test", "a", "b", func(a, b rel.Var) rel.Clauses {
			c := rel.Var("c")
			return rel.Clauses{
				a.AttrEqVar(rel.Self, b),
				c.AttrEqVar(rel.Self, a),
			}
		})
	})
	sc.Def2("test", "a", "b", func(a, b rel.Var) rel.Clauses {
		return rel.Clauses{a.AttrEqVar(rel.Self, b)}
	})
	require.PanicsWithError(t, "already registered rule with name test", func() {
		sc.Def2("test", "a", "b", func(a, b rel.Var) rel.Clauses {
			return rel.Clauses{a.AttrEqVar(rel.Self, b)}
		})
	})
}

// TestEmbeddedFieldsWork is a sanity check that the logic to use
// embedded fields works correctly. Ensure also that embedded pointers
// are not supported.
func TestEmbeddedFieldsWork(t *testing.T) {
	type embed2 struct {
		C int32
	}
	type Embed struct {
		B int32
		embed2
	}
	type outer struct {
		A int32
		Embed
		D int32
		embed2
	}
	var a, b, c1, c2, d stringAttr = "a", "b", "c1", "c2", "d"
	t.Run("basic", func(t *testing.T) {
		sc := rel.MustSchema("rules",
			rel.EntityMapping(reflect.TypeOf((*outer)(nil)),
				rel.EntityAttr(a, "A"),
				rel.EntityAttr(b, "B"),
				rel.EntityAttr(c1, "C"),
				rel.EntityAttr(c2, "Embed.C"),
				rel.EntityAttr(d, "D"),
			),
		)
		v := &outer{}
		v.A = 1
		v.B = 2
		v.C = 3
		v.Embed.C = 4
		v.D = 5
		checkAttr := func(ent interface{}, attr stringAttr, exp int32) {
			got, err := sc.GetAttribute(attr, ent)
			require.NoError(t, err)
			require.Equal(t, exp, got)
		}
		checkAttr(v, a, 1)
		checkAttr(v, b, 2)
		checkAttr(v, c1, 3)
		checkAttr(v, c2, 4)
		checkAttr(v, d, 5)
	})
	t.Run("pointer embedding is not supported", func(t *testing.T) {
		type outerOuter struct {
			*outer
			B int32
		}
		_, err := rel.NewSchema("rules",
			rel.EntityMapping(reflect.TypeOf((*outerOuter)(nil)),
				rel.EntityAttr(a, "A"),
				rel.EntityAttr(b, "B"),
				rel.EntityAttr(c1, "C"),
				rel.EntityAttr(c2, "Embed.C"),
			),
		)
		require.EqualError(t, err,
			"failed to construct schema: *rel_test.outerOuter.A references an "+
				"embedded pointer outer")
	})
}

// TestConcurrentQueryInDifferentDatabases stresses some logic of the
// evalContext pooling to ensure that the state is properly reset between
// queries. An important property of this test is that it uses an any
// clause over entity pointers. When these pointers are inlined in the
// context of different databases, they will have different values. By
// randomizing the insertion order, we ensure that the inline values for
// the different entities differ.
//
// This test exercises the code which resets the inline values of slots in
// in the evalContext corresponding to the "not" and "any" constraints on that
// slot. These fields are pointers and need to be reset explicitly. The bug
// which motivated this test was that the slots were only being reset by value.
func TestConcurrentQueryInDifferentDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type entity struct {
		Str   string
		Other *entity
	}
	var str, other stringAttr = "str", "other"
	schema := rel.MustSchema("test",
		rel.EntityMapping(
			reflect.TypeOf((*entity)(nil)),
			rel.EntityAttr(str, "Str"),
			rel.EntityAttr(other, "Other"),
		),
	)
	newDB := func() *rel.Database {
		db, err := rel.NewDatabase(schema, rel.Index{Attrs: []rel.Attr{other}})
		require.NoError(t, err)
		return db
	}
	const (
		numDBs          = 3
		numEntities     = 5
		numContainsVals = 3
	)
	makeEntities := func() (ret []*entity) {
		for i := 0; i < numEntities; i++ {
			ret = append(ret, &entity{Str: fmt.Sprintf("s%d", i)})
		}
		for i := 0; i < numEntities; i++ {
			ret[i].Other = ret[(i+1)%numEntities]
		}
		return ret
	}
	makeDBs := func() (ret []*rel.Database) {
		for i := 0; i < numDBs; i++ {
			ret = append(ret, newDB())
		}
		return ret
	}
	addEntitiesToDB := func(db *rel.Database, entities []*entity) {
		for _, i := range rand.Perm(len(entities)) {
			require.NoError(t, db.Insert(entities[i]))
		}
	}
	addEntitiesToDBs := func(dbs []*rel.Database, entities []*entity) {
		for _, db := range dbs {
			addEntitiesToDB(db, entities)
		}
	}

	dbs, entities := makeDBs(), makeEntities()
	addEntitiesToDBs(dbs, entities)
	assert.Less(t, numContainsVals, numEntities)
	makeContainsVals := func(entities []*entity) (ret []interface{}) {
		for i := 0; i < numContainsVals; i++ {
			ret = append(ret, entities[i+1])
		}
		return ret
	}
	type v = rel.Var
	q, err := rel.NewQuery(schema,
		v("e").AttrIn(other, makeContainsVals(entities)...),
		v("e").AttrNeq(rel.Self, entities[0]), // exclude the first entity
	)
	require.NoError(t, err)
	var N = 8
	exp := entities[1:numContainsVals] // the first entity is excluded
	run := func(i int) func() error {
		return func() error {
			var got []*entity
			assert.NoError(t, q.Iterate(dbs[i%len(dbs)], nil, func(r rel.Result) error {
				got = append(got, r.Var("e").(*entity))
				return nil
			}))
			assert.EqualValues(t, exp, got)
			return nil
		}
	}
	var g errgroup.Group
	for i := 0; i < N; i++ {
		g.Go(run(i))
	}
	require.NoError(t, g.Wait())
}
