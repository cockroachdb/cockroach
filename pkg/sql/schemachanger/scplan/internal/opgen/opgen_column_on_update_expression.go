// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnOnUpdateExpression)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.AddColumnOnUpdateExpression {
					return &scop.AddColumnOnUpdateExpression{
						OnUpdate: *protoutil.Clone(this).(*scpb.ColumnOnUpdateExpression),
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:            this.UsesSequenceIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.RemoveColumnOnUpdateExpression {
					return &scop.RemoveColumnOnUpdateExpression{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:            this.UsesSequenceIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
			),
		),
	)
}
