// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register(
		(*scpb.ColumnFamily)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnFamily) *scop.AddColumnFamily {
					return &scop.AddColumnFamily{
						TableID:  this.TableID,
						FamilyID: this.FamilyID,
						Name:     this.Name,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.ColumnFamily, md *opGenContext) *scop.AssertColumnFamilyIsRemoved {
					// Use stricter criteria on 23.1, which will guarantee that the
					// column family is cleaned up first using the column type element.
					// The only purpose this operation serves is to make sure that this
					// when the element reaches absent the column family has actually
					// been removed. This would have been done via the last column
					// type element referencing it.
					if md.ActiveVersion.IsActive(clusterversion.TODODelete_V23_1) {
						return &scop.AssertColumnFamilyIsRemoved{
							TableID:  this.TableID,
							FamilyID: this.FamilyID,
						}
					}
					// We would have used NotImplemented before, but that will be an
					// assertion now, so just return no-ops.
					return nil
				}),
			),
		),
	)
}
