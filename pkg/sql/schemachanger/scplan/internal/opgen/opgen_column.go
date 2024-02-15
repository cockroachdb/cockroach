// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
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
			to(scpb.Status_PUBLIC_BUT_INACCESSIBLE,
				emit(func(this *scpb.Column, md *opGenContext) *scop.MakeWriteOnlyColumnPublicButInaccessible {
					return &scop.MakeWriteOnlyColumnPublicButInaccessible{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.Column, md *opGenContext) *scop.MakePublicButInaccessibleColumnPublic {
					if this.IsInaccessible {
						// Do not make the column accessible if it's meant to be an inaccessible column.
						return nil
					}
					return &scop.MakePublicButInaccessibleColumnPublic{
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
			to(scpb.Status_PUBLIC_BUT_INACCESSIBLE,
				emit(func(this *scpb.Column) *scop.MakePublicColumnPublicButInaccessible {
					return &scop.MakePublicColumnPublicButInaccessible{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.Column) *scop.MakePublicButInaccessibleColumnWriteOnly {
					return &scop.MakePublicButInaccessibleColumnWriteOnly{
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
