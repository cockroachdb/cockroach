// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package current

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
)

func init() {
	// This rule ensures that the schema_locked element is unset before any
	// operation that adds elements.
	registerDepRule("schema locked is unset before any operation (add)",
		scgraph.Precedence,
		"schema-locked", "descriptor-element",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TableSchemaLocked)(nil)),
				to.TypeFilter(rulesVersionKey, Not(isSchemaLocked)),
				JoinOnDescID(from, to, "descID"),
				TransientPublicPrecedesInitialPublic(from, to),
			}
		})
	// This rule ensures that the schema_locked element is unset before any
	// operation that drops elements.
	registerDepRule("schema locked is unset before any operation (drop)",
		scgraph.Precedence,
		"schema-locked", "descriptor-element",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.Type((*scpb.TableSchemaLocked)(nil)),
				to.TypeFilter(rulesVersionKey, Not(isSchemaLocked)),
				JoinOnDescID(from, to, "descID"),
				TransientPublicPrecedesInitialDrop(from, to),
			}
		})

	// This rule ensures that the schema_locked element is set after all terminal
	// drop operations
	registerDepRule("schema locked is set after all other operations (drop)",
		scgraph.PreviousTransactionPrecedence,
		"descriptor-element", "schema-locked",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, Not(isSchemaLocked)),
				to.Type((*scpb.TableSchemaLocked)(nil)),
				JoinOnDescID(from, to, "descID"),
				// Schema locked can only happen after terminal states
				DropTerminalPrecedesTransientPublic(from, to),
			}
		})

	// This rule ensures that the schema_locked element is set after all terminal
	// drop operations.
	registerDepRule("schema locked is set after all other operations (add)",
		scgraph.PreviousTransactionPrecedence,
		"descriptor-element", "schema-locked",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, Not(isSchemaLocked)),
				to.Type((*scpb.TableSchemaLocked)(nil)),
				JoinOnDescID(from, to, "descID"),
				// Schema locked can only happen after terminal states
				PublicTerminalPrecedesTransientPublic(from, to),
			}
		})
}
