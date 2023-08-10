// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils/tsql"
)

const (
	descPB = "cockroach.sql.sqlbase.Descriptor"
)

// DescriptorCorruption defines and showcases a specific "class" of descriptor
// corruption.
// It exists as a test utility to help enumerate, document, and reproduce
// corruptions that occur in the wild.
type DescriptorCorruption struct {
	// Corruptor returns a tree.Expr that applies this "class" of corruption to
	// the provided descriptor.
	// For example, outbound foreign key references may be removed by wrapping
	// the provided tree.Expr in json_remove_path(expr, 'table', 'outboundFKs').
	Corruptor func(descJSON tree.Expr) tree.Expr
	// Where return a tree.Expr that will be AND'd into a WHERE clause. The
	// returned expression should limit system.descriptor rows to those that can
	// be corrupted by this class.
	// For example, if removing outboundFKs, the returned tree.Expr should
	// exclude non-table descriptors and table descriptors without outboundFKs.
	Where func(descJSON tree.Expr) tree.Expr
}

var (
	// MissingInboundFKsCorruption is a DescriptorCorruption where a Table
	// descriptor is missing some, or all, inbound foreign key references.
	MissingInboundFKsCorruption = DescriptorCorruption{
		Corruptor: func(descJSON tree.Expr) tree.Expr {
			return tsql.JSONRemovePath(descJSON, "table", "inboundFKs")
		},
		Where: func(descJSON tree.Expr) tree.Expr {
			// descriptor_json ? 'table' AND json_array_length(descriptor_json -> table -> inboundFKs) > 0
			return tsql.And(
				// Only apply to tables.,
				tsql.JSONExists(descJSON, "table"),
				// Only select descriptors that have inbound FKs.
				tsql.Cmp(
					tsql.JSONArrayLength(tsql.JSONExtractPath(descJSON, "table", "inboundFKs")),
					tsql.GT,
					tree.NewDInt(0),
				),
			)
		},
	}

	// MissingInboundFKsCorruption is a DescriptorCorruption where a Table
	// descriptor is missing some, or all, outbound foreign key references.
	MissingOutboundFKsCorruption = DescriptorCorruption{
		Corruptor: func(descJSON tree.Expr) tree.Expr {
			return tsql.JSONRemovePath(descJSON, "table", "outboundFKs")
		},
		Where: func(descJSON tree.Expr) tree.Expr {
			// descriptor_json ? 'table' AND json_array_length(descriptor_json -> table -> inboundFKs) > 0
			return tsql.And(
				// Only apply to tables.
				tsql.JSONExists(descJSON, "table"),
				// Only select descriptors that have outbound FKs.
				tsql.Cmp(
					tsql.JSONArrayLength(tsql.JSONExtractPath(descJSON, "table", "outboundFKs")),
					tsql.GT,
					tree.NewDInt(0),
				),
			)
		},
	}

	// TODO InvalidMutationJobCorruption can apply to any descriptor type. It
	// only applies to tables for now as the query is a bit more involved.
	InvalidMutationJobCorruption = DescriptorCorruption{
		Corruptor: func(descJSON tree.Expr) tree.Expr {
			return tsql.JSONSet(
				descJSON,
				[]string{"table", "mutations"},
				tsql.JSONBuildArray(
					tsql.JSONBuildObject(map[string]any{
						"mutation_id": 1,
					}),
				),
			)
		},
		Where: func(descJSON tree.Expr) tree.Expr {
			return tsql.JSONExists(descJSON, "table")
		},
	}

	InvalidMutationCorruption = DescriptorCorruption{
	}
)

type DescriptorTarget interface {

}

func MustCorrupt(t Fataler, sr *SQLRunner, target DescriptorTarget, corruptions ...DescriptorCorruption) {
}

// MustCorruptOne applies the given DescriptorCorruption and asserts that
// exactly one descriptor was corrupted. It will call t.Fatal, if no
// descriptors are susceptible by the provided corruption class.
func MustCorruptOne(t Fataler, sr *SQLRunner, c DescriptorCorruption, where ...tree.Expr) {
	idCol := tree.NewUnresolvedName("id")
	descCol := tree.NewUnresolvedName("descriptor")
	descJSON := tsql.PBToJSON(descPB, descCol)
	descTbl := tree.MakeTableNameFromPrefix(tree.ObjectNamePrefix{SchemaName: "system", ExplicitSchema: true}, "descriptor")

	where = append(where,
		// Filter out system descriptors.
		tsql.Cmp(idCol, tsql.GT, tree.NewDInt(105)),
		c.Where(descCol),
	)

	stmt := tree.Select{
		Select: &tree.SelectClause{
			Exprs: []tree.SelectExpr{
				{
					Expr: descJSON,
					// Expr: tsql.UnsafeUpsertDescriptor(
					// 	idCol,
					// 	tsql.JSONToPB(descPB, c.Corruptor(descJSON)),
					// 	true,
					// ),
				},
			},
			From: tree.From{
				Tables: []tree.TableExpr{&descTbl},
			},
			Where: &tree.Where{
				Type: tree.AstWhere,
				Expr: tsql.And(where...),
			},
		},
		// Limit one so we only corrupt at most 1 descriptor.
		Limit: &tree.Limit{Count: tree.NewDInt(1)},
	}

	sr.ExecRowsAffected(t, 1, tsql.ToString(&stmt))
}
