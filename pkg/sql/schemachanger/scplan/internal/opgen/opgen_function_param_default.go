// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.FunctionParamDefaultExpression)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.SetFunctionParamDefaultExpr {
					d := protoutil.Clone(this).(*scpb.FunctionParamDefaultExpression)
					return &scop.SetFunctionParamDefaultExpr{
						Expr: *d,
					}
				}),
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.UpdateFunctionTypeReferences {
					return &scop.UpdateFunctionTypeReferences{
						FunctionID: this.FunctionID,
						TypeIDs:    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.UpdateFunctionRelationReferences {
					return &scop.UpdateFunctionRelationReferences{
						FunctionID:         this.FunctionID,
						SequenceIDs:        this.UsesSequenceIDs,
						FunctionReferences: this.UsesFunctionIDs,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.RemoveBackReferenceInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferenceInTypes{
						BackReferencedDescriptorID: this.FunctionID,
						TypeIDs:                    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.RemoveBackReferencesInRelations {
					if len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferencesInRelations{
						BackReferencedID: this.FunctionID,
						RelationIDs:      this.UsesSequenceIDs,
					}
				}),
				emit(func(this *scpb.FunctionParamDefaultExpression) *scop.RemoveBackReferenceInFunctions {
					return &scop.RemoveBackReferenceInFunctions{
						BackReferencedDescriptorID: this.FunctionID,
						FunctionIDs:                append([]descpb.ID(nil), this.UsesFunctionIDs...),
					}
				}),
			),
		),
	)
}
