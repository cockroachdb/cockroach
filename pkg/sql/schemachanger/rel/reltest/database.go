// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reltest

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// DatabaseTest tests a set of queries in the context of a database which
// has items specified in Data in a Database.
type DatabaseTest struct {

	// Data is the set of data stored in the registry to insert
	// into the database for the purpose of querying.
	Data []string
	// Each of the QueryCases will be run with the set of indexes.
	Indexes [][]rel.Index

	QueryCases []QueryTest
}

// QueryTest is a subtest of a DatabaseTest which ensures that the results
// of a query match the expectations.
type QueryTest struct {
	// Name is the name of the subtest.
	Name string
	// Query are the clauses of the query.
	Query rel.Clauses

	// ResVars are the variables which should be examined as a part of the
	// result.
	ResVars []rel.Var

	// Results are the expected set of results. Order does not matter.
	Results [][]interface{}

	// Entities is the set of entities of the query in their join order.
	Entities []rel.Var

	// ErrorRE is used to indicate that the query is invalid and will
	// result in an error that must match this pattern.
	ErrorRE string

	// UnsatisfiableIndexes are the indexes which cannot satisfy the query.
	UnsatisfiableIndexes []int
}

func (tc DatabaseTest) run(t *testing.T, s Suite) {
	for i, databaseIndexes := range tc.databaseIndexes() {
		t.Run(fmt.Sprintf("%v", i), func(t *testing.T) {
			db, err := rel.NewDatabase(s.Schema, databaseIndexes...)
			require.NoError(t, err)
			for _, k := range tc.Data {
				v := s.Registry.MustGetByName(t, k)
				require.NoError(t, db.Insert(v))
			}
			for _, qc := range tc.QueryCases {
				t.Run(qc.Name, func(t *testing.T) {
					qc.run(t, i, db)
				})
			}
		})
	}
}

func (qc QueryTest) run(t *testing.T, indexes int, db *rel.Database) {
	var results [][]interface{}
	q, err := rel.NewQuery(db.Schema(), qc.Query...)
	if qc.ErrorRE != "" {
		require.Regexp(t, qc.ErrorRE, err)
		return
	}

	require.NoError(t, err)
	require.Equal(t, qc.Entities, q.Entities())
	stats := &rel.QueryStats{}
	if err := q.Iterate(db, stats, func(r rel.Result) error {
		var cur []interface{}
		for _, v := range qc.ResVars {
			cur = append(cur, r.Var(v))
		}
		results = append(results, cur)
		return nil
	}); testutils.IsError(err, `failed to find index to satisfy query`) {
		if intsets.MakeFast(qc.UnsatisfiableIndexes...).Contains(indexes) {
			return
		}
		t.Fatalf("expected to succeed with indexes %d: %v", indexes, err)
	} else if err != nil {
		t.Fatal(err)
	} else if intsets.MakeFast(qc.UnsatisfiableIndexes...).Contains(indexes) {
		t.Fatalf("expected to fail with indexes %d", indexes)
	}
	if len(results) == 0 {
		require.Equal(t, 0, stats.ResultsFound)
		require.Less(t, stats.FirstUnsatisfiedClause, len(q.Clauses()),
			"There are %d clauses in the query, but the first unsatisfied clause is %d",
			len(q.Clauses()), stats.FirstUnsatisfiedClause)
	}
	expResults := append(qc.Results[:0:0], qc.Results...)
	findResultInExp := func(res []interface{}) (found bool) {
		for i, exp := range expResults {
			if reflect.DeepEqual(exp, res) {
				expResults = append(expResults[:i], expResults[i+1:]...)
				return true
			}
		}
		return false
	}

	for _, res := range results {
		if !findResultInExp(res) {
			t.Fatalf("failed to find %v in %v", res, expResults)
		}
	}
	require.Empty(t, expResults, "got", results)
}

func (tc DatabaseTest) encode(t *testing.T, r *Registry) *yaml.Node {
	return &yaml.Node{
		Kind: yaml.MappingNode,
		Content: []*yaml.Node{
			scalarYAML("indexes"),
			tc.encodeIndexes(r),
			scalarYAML("data"),
			tc.encodeData(),
			scalarYAML("queries"),
			tc.encodeQueries(t, r),
		},
	}
}

func (tc DatabaseTest) encodeIndexes(r *Registry) *yaml.Node {
	databaseIndexesNode := yaml.Node{Kind: yaml.MappingNode}
	for i, indexes := range tc.databaseIndexes() {
		indexesNode := yaml.Node{Kind: yaml.SequenceNode}
		for _, idx := range indexes {
			// TODO(ajwerner): Implement encoding of the other index features.
			indexesNode.Content = append(indexesNode.Content, encodeIdx(idx))
		}
		databaseIndexesNode.Content = append(databaseIndexesNode.Content,
			scalarYAML(strconv.Itoa(i)),
			&indexesNode)
	}
	return &databaseIndexesNode
}

func encodeIdx(idx rel.Index) *yaml.Node {
	indexNode := yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
	indexNode.Content = []*yaml.Node{
		scalarYAML("attrs"),
		encodeAttrs(idx.Attrs),
	}
	if len(idx.Exists) > 0 {
		indexNode.Content = append(indexNode.Content,
			scalarYAML("exists"),
			encodeAttrs(idx.Exists),
		)
	}
	if len(idx.Where) > 0 {
		clause := yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle}
		for _, w := range idx.Where {
			var n yaml.Node
			if w.Attr == rel.Type {
				n = *scalarYAML(fmt.Sprintf("%v", w.Eq))
			} else if err := n.Encode(w.Eq); err != nil {
				n = *scalarYAML("ERROR: " + err.Error())
			}
			clause.Content = append(clause.Content,
				scalarYAML(w.Attr.String()), &n)
		}
		indexNode.Content = append(indexNode.Content,
			scalarYAML("where"),
			&clause,
		)
	}
	return &indexNode
}

func encodeAttrs(idx []rel.Attr) *yaml.Node {
	indexNode := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, attr := range idx {
		indexNode.Content = append(indexNode.Content, scalarYAML(attr.String()))
	}
	return &indexNode
}

func (tc DatabaseTest) encodeData() *yaml.Node {
	dataNode := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, k := range tc.Data {
		dataNode.Content = append(dataNode.Content, scalarYAML(k))
	}
	return &dataNode
}

func (tc DatabaseTest) encodeQueries(t *testing.T, r *Registry) *yaml.Node {
	queriesNode := yaml.Node{Kind: yaml.MappingNode}

	encodeValues := func(t *testing.T, v []interface{}) *yaml.Node {
		var seq yaml.Node
		seq.Kind = yaml.SequenceNode
		seq.Style = yaml.FlowStyle
		for _, v := range v {
			name, ok := r.GetName(v)
			if ok {
				seq.Content = append(seq.Content, scalarYAML(name))
			} else if typ, isType := v.(reflect.Type); isType {
				seq.Content = append(seq.Content, scalarYAML(typ.String()))
			} else {
				var content yaml.Node
				require.NoError(t, content.Encode(v))
				seq.Content = append(seq.Content, &content)
			}
		}
		return &seq
	}
	encodeResults := func(t *testing.T, results [][]interface{}) *yaml.Node {
		var res yaml.Node
		res.Kind = yaml.SequenceNode
		for _, r := range results {
			res.Content = append(res.Content, encodeValues(t, r))
		}
		return &res
	}
	encodeVars := func(vars []rel.Var) *yaml.Node {
		n := yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
		for _, v := range vars {
			n.Content = append(n.Content, scalarYAML("$"+string(v)))
		}
		return &n
	}
	addQuery := func(t *testing.T, qt QueryTest) {
		var query yaml.Node
		require.NoError(t, query.Encode(qt.Query))

		var qtNode *yaml.Node
		if qt.ErrorRE != "" {
			qtNode = &yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalarYAML("query"),
					&query,
					scalarYAML("error"),
					scalarYAML(qt.ErrorRE),
				},
			}
		} else {
			qtNode = &yaml.Node{
				Kind: yaml.MappingNode,
				Content: []*yaml.Node{
					scalarYAML("query"),
					&query,
					scalarYAML("entities"),
					encodeVars(qt.Entities),
					scalarYAML("result-vars"),
					encodeVars(qt.ResVars),
					scalarYAML("results"),
					encodeResults(t, qt.Results),
				},
			}
			if len(qt.UnsatisfiableIndexes) > 0 {
				var n yaml.Node
				_ = n.Encode(qt.UnsatisfiableIndexes)
				n.Style = yaml.FlowStyle
				qtNode.Content = append(qtNode.Content,
					scalarYAML("unsatisfiableIndexes"), &n)
			}
		}

		queriesNode.Content = append(queriesNode.Content,
			scalarYAML(qt.Name), qtNode,
		)
	}
	for _, qc := range tc.QueryCases {
		addQuery(t, qc)
	}
	return &queriesNode
}

func (tc DatabaseTest) databaseIndexes() [][]rel.Index {
	if len(tc.Indexes) == 0 {
		return [][]rel.Index{{}}
	}
	return tc.Indexes
}
