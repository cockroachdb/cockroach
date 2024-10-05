// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func init() {
	opRegistry.register((*scpb.Table)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DROPPED,
				emit(func(this *scpb.Table) *scop.NotImplemented {
					return notImplemented(this)
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsPublic {
					return &scop.MarkDescriptorAsPublic{
						DescriptorID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_DROPPED,
				revertible(false),
				emit(func(this *scpb.Table) *scop.MarkDescriptorAsDropped {
					return &scop.MarkDescriptorAsDropped{
						DescriptorID: this.TableID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Table, md *opGenContext) *scop.CreateGCJobForTable {
					if !md.ActiveVersion.IsActive(clusterversion.V23_1) {
						return &scop.CreateGCJobForTable{
							TableID:             this.TableID,
							DatabaseID:          databaseIDFromDroppedNamespaceTarget(md, this.TableID),
							StatementForDropJob: statementForDropJob(this, md),
						}
					}
					return nil
				}),
			),
		),
	)
}

func databaseIDFromDroppedNamespaceTarget(md *opGenContext, objectID descpb.ID) descpb.ID {
	for _, t := range md.Targets {
		switch e := t.Element().(type) {
		case *scpb.Namespace:
			if t.TargetStatus != scpb.ToPublic.Status() && e.DescriptorID == objectID {
				return e.DatabaseID
			}
		}
	}
	panic(errors.AssertionFailedf("no ABSENT scpb.Namespace target found with object ID %d", objectID))
}
