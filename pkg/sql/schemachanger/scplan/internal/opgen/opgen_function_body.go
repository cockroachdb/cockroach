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
	opRegistry.register((*scpb.FunctionBody)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.FunctionBody) *scop.SetFunctionBody {
					f := protoutil.Clone(this).(*scpb.FunctionBody)
					return &scop.SetFunctionBody{
						Body: *f,
					}
				}),
				emit(func(this *scpb.FunctionBody) *scop.UpdateFunctionTypeReferences {
					return &scop.UpdateFunctionTypeReferences{
						FunctionID: this.FunctionID,
						TypeIDs:    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.FunctionBody) *scop.UpdateFunctionRelationReferences {
					return &scop.UpdateFunctionRelationReferences{
						FunctionID:      this.FunctionID,
						TableReferences: this.UsesTables,
						ViewReferences:  this.UsesViews,
						SequenceIDs:     this.UsesSequenceIDs,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.FunctionBody) *scop.RemoveBackReferenceInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferenceInTypes{
						BackReferencedDescriptorID: this.FunctionID,
						TypeIDs:                    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.FunctionBody) *scop.RemoveBackReferencesInRelations {
					var relationIDs []descpb.ID
					for _, ref := range this.UsesTables {
						relationIDs = append(relationIDs, ref.TableID)
					}
					for _, ref := range this.UsesViews {
						relationIDs = append(relationIDs, ref.ViewID)
					}
					relationIDs = append(relationIDs, this.UsesSequenceIDs...)
					if len(relationIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferencesInRelations{
						BackReferencedID: this.FunctionID,
						RelationIDs:      relationIDs,
					}
				})),
		),
	)
}
