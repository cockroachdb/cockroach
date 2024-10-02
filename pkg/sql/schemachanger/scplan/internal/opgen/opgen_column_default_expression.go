// Copyright 2021 The Cockroach Authors.
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
	opRegistry.register((*scpb.ColumnDefaultExpression)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnDefaultExpression) *scop.AddColumnDefaultExpression {
					return &scop.AddColumnDefaultExpression{
						Default: *protoutil.Clone(this).(*scpb.ColumnDefaultExpression),
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:            this.UsesSequenceIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.AddTableColumnBackReferencesInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.AddTableColumnBackReferencesInFunctions{
						FunctionIDs:            this.UsesFunctionIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnDefaultExpression) *scop.RemoveColumnDefaultExpression {
					return &scop.RemoveColumnDefaultExpression{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:            this.UsesSequenceIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnDefaultExpression) *scop.RemoveTableColumnBackReferencesInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.RemoveTableColumnBackReferencesInFunctions{
						FunctionIDs:            this.UsesFunctionIDs,
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
			),
		),
	)
}
