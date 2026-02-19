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

// These rules ensure that ALTER TABLE LOCALITY operations proceed correctly,
// managing dependencies between locality elements, zone configs, columns, and
// primary key changes.

func init() {
	// Old locality element must be absent before new locality element becomes public
	// This ensures we don't have two conflicting locality configurations at the same time.
	registerDepRule(
		"old locality must become absent before the new locality is public",
		scgraph.Precedence,
		"old-locality", "new-locality",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTableLocalityElement),
				to.TypeFilter(rulesVersionKey, isTableLocalityElement),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_PUBLIC),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_ABSENT),
			}
		},
	)
}

// isTableLocalityElement returns true for any table locality element type.
func isTableLocalityElement(element scpb.Element) bool {
	switch element.(type) {
	case *scpb.TableLocalityGlobal,
		*scpb.TableLocalityPrimaryRegion,
		*scpb.TableLocalitySecondaryRegion,
		*scpb.TableLocalityRegionalByRow:
		return true
	}
	return false
}
