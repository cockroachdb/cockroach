// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.Column)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_DELETE_ONLY,
				emit(func(this *scpb.Column) *scop.MakeAbsentColumnDeleteOnly {
					return &scop.MakeAbsentColumnDeleteOnly{
						Column: *protoutil.Clone(this).(*scpb.Column),
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.Column) *scop.MakeDeleteOnlyColumnWriteOnly {
					return &scop.MakeDeleteOnlyColumnWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertibleFunc(func(e scpb.Element, state *opGenContext) bool {
					return checkIfDescriptorIsWithoutData(screl.GetDescID(e), state)
				}),
				emit(func(this *scpb.Column, md *opGenContext) *scop.MakeWriteOnlyColumnPublic {
					return &scop.MakeWriteOnlyColumnPublic{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.Column, md *opGenContext) *scop.RefreshStats {
					// No need to generate stats for empty descriptors.
					if checkIfDescriptorIsWithoutData(this.TableID, md) {
						return nil
					}
					return &scop.RefreshStats{
						TableID: this.TableID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.Column) *scop.MakePublicColumnWriteOnly {
					return &scop.MakePublicColumnWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_DELETE_ONLY,
				revertible(false),
				emit(func(this *scpb.Column) *scop.MakeWriteOnlyColumnDeleteOnly {
					return &scop.MakeWriteOnlyColumnDeleteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Column, md *opGenContext) *scop.MakeDeleteOnlyColumnAbsent {
					return &scop.MakeDeleteOnlyColumnAbsent{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
		),
	)
}
