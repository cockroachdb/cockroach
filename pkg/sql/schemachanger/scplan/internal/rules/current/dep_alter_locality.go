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
	// Old locality must become absent right before the new locality is public.
	// This is to ensure that locality will change to its target value in one step.
	registerDepRule(
		"old locality must become absent right before the new locality is public",
		scgraph.SameStagePrecedence,
		"old-locality", "new-locality",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTableLocalityElement),
				to.TypeFilter(rulesVersionKey, isTableLocalityElement),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// New indexes must become public before the new locality becomes public.
	// This ensures that when altering locality (e.g., to REGIONAL BY ROW),
	// all new indexes (primary and secondary with new partitioning) are fully
	// available before the locality change is visible.
	registerDepRule(
		"new indexes must become public before new locality is public",
		scgraph.Precedence,
		"index", "new-locality",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isIndex),
				to.TypeFilter(rulesVersionKey, isTableLocalityElement),
				JoinOnDescID(from, to, "table-id"),
				from.TargetStatus(scpb.ToPublic),
				from.CurrentStatus(scpb.Status_PUBLIC),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// New locality must become public right before the new zone config.
	// This ensures the zone configuration is updated at the same time as
	// the locality change takes effect.
	registerDepRule(
		"locality must become public right before table zone config is public",
		scgraph.SameStagePrecedence,
		"new-locality", "new-zone-config",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTableLocalityElement),
				to.Type((*scpb.TableZoneConfig)(nil)),
				JoinOnDescID(from, to, "table-id"),
				to.TargetStatus(scpb.ToPublic),
				to.CurrentStatus(scpb.Status_PUBLIC),
				from.TargetStatus(scpb.ToPublic),
				from.CurrentStatus(scpb.Status_PUBLIC),
			}
		},
	)

	// New locality must become public right before the old zone config goes absent.
	// This ensures the zone configuration is updated at the same time as
	// the locality change takes effect.
	registerDepRule(
		"locality must become public right before old table zone config is absent",
		scgraph.SameStagePrecedence,
		"new-locality", "new-zone-config",
		func(from, to NodeVars) rel.Clauses {
			return rel.Clauses{
				from.TypeFilter(rulesVersionKey, isTableLocalityElement),
				to.Type((*scpb.TableZoneConfig)(nil)),
				JoinOnDescID(from, to, "table-id"),
				to.TargetStatus(scpb.ToAbsent),
				to.CurrentStatus(scpb.Status_ABSENT),
				from.TargetStatus(scpb.ToAbsent),
				from.CurrentStatus(scpb.Status_ABSENT),
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
