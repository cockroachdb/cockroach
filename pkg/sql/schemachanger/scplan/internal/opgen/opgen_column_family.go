// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
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
					// Use stricter criteria, which will guarantee that the column family
					// is cleaned up first using the column type element. The only purpose
					// this operation serves is to make sure that this when the element
					// reaches absent the column family has actually been removed. This
					// would have been done via the last column type element referencing
					// it.
					return &scop.AssertColumnFamilyIsRemoved{
						TableID:  this.TableID,
						FamilyID: this.FamilyID,
					}
				}),
			),
		),
	)
}
