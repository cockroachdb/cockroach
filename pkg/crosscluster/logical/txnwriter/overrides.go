// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnwriter

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// DestTableOverrides returns a DescriptorOverrides map granting the LDR job
// owner implicit SELECT, INSERT, UPDATE, and DELETE on every
// destination table, and exempts those descriptors from row-level
// security policies. LDR sessions run as the job owner (the user who
// created the stream), who holds REPLICATIONDEST but not necessarily
// DML grants on the destinations; the override gives them just enough
// to apply replicated rows without widening their authority anywhere
// else. RLS bypass is required because a deny-all policy on the
// destination would otherwise filter out the rows the writer is
// applying or block WITH CHECK on inserts, routing every replicated
// row to the DLQ. Attach the result to a SessionData (for paths that
// hand the session to the applier directly) or to an
// InternalExecutorOverride (for paths that use per-op overrides).
func DestTableOverrides(tableIDs []descpb.ID) map[uint32]sessiondata.DescriptorOverride {
	bits := privilege.List{
		privilege.SELECT, privilege.INSERT, privilege.UPDATE, privilege.DELETE,
	}.ToBitField()
	grants := make(map[uint32]sessiondata.DescriptorOverride, len(tableIDs))
	for _, id := range tableIDs {
		grants[uint32(id)] = sessiondata.DescriptorOverride{Privileges: bits, BypassRLS: true}
	}
	return grants
}
