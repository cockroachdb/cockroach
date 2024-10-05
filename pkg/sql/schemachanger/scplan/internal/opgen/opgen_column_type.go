// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnType)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnType) *scop.SetAddedColumnType {
					return &scop.SetAddedColumnType{
						ColumnType: *protoutil.Clone(this).(*scpb.ColumnType),
					}
				}),
				emit(func(this *scpb.ColumnType) *scop.UpdateTableBackReferencesInTypes {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						return &scop.UpdateTableBackReferencesInTypes{
							TypeIDs:               ids,
							BackReferencedTableID: this.TableID,
						}
					}
					return nil
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.ColumnType) *scop.RemoveDroppedColumnType {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						return &scop.RemoveDroppedColumnType{
							TableID:  this.TableID,
							ColumnID: this.ColumnID,
						}
					}
					return nil
				}),
				emit(func(this *scpb.ColumnType) *scop.UpdateTableBackReferencesInTypes {
					if ids := referencedTypeIDs(this); len(ids) > 0 {
						return &scop.UpdateTableBackReferencesInTypes{
							TypeIDs:               ids,
							BackReferencedTableID: this.TableID,
						}
					}
					return nil
				}),
			),
		),
	)
}

func referencedTypeIDs(this *scpb.ColumnType) []catid.DescID {
	var ids catalog.DescriptorIDSet
	if this.ComputeExpr != nil {
		for _, id := range this.ComputeExpr.UsesTypeIDs {
			ids.Add(id)
		}
	}
	for _, id := range this.ClosedTypeIDs {
		ids.Add(id)
	}
	return ids.Ordered()
}
