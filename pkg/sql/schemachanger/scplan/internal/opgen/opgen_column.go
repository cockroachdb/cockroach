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
			to(scpb.Status_PUBLIC,
				revertible(false),
				emit(func(this *scpb.Column, md *opGenContext) *scop.MakeWriteOnlyColumnPublic {
					return &scop.MakeWriteOnlyColumnPublic{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.Column) *scop.RefreshStats {
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
