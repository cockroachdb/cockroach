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
	opRegistry.register((*scpb.ColumnNotNull)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.ColumnNotNull) *scop.MakeAbsentColumnNotNullWriteOnly {
					return &scop.MakeAbsentColumnNotNullWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.ColumnNotNull, md *opGenContext) *scop.ValidateColumnNotNull {
					if checkIfDescriptorIsWithoutData(this.TableID, md) {
						return nil
					}
					return &scop.ValidateColumnNotNull{
						TableID:              this.TableID,
						ColumnID:             this.ColumnID,
						IndexIDForValidation: this.IndexIDForValidation,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnNotNull) *scop.MakeValidatedColumnNotNullPublic {
					return &scop.MakeValidatedColumnNotNullPublic{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_VALIDATED,
				emit(func(this *scpb.ColumnNotNull) *scop.MakePublicColumnNotNullValidated {
					return &scop.MakePublicColumnNotNullValidated{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			equiv(scpb.Status_WRITE_ONLY),
			to(scpb.Status_ABSENT,
				revertible(false),
				emit(func(this *scpb.ColumnNotNull) *scop.RemoveColumnNotNull {
					return &scop.RemoveColumnNotNull{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
		),
	)
}
