// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.ViewQuery)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ViewQuery) *scop.SetViewQuery {
					return &scop.SetViewQuery{
						ViewID: this.ViewID,
						Query:  this.Query,
					}
				}),
				emit(func(this *scpb.ViewQuery) *scop.UpdateViewBackReferencesInRelations {
					if len(this.UsesTables) == 0 && len(this.UsesViews) == 0 && len(this.UsesSequenceIDs) == 0 {
						return nil
					}
					return &scop.UpdateViewBackReferencesInRelations{
						ViewID:          this.ViewID,
						TableReferences: this.UsesTables,
						ViewReferences:  this.UsesViews,
						SequenceIDs:     this.UsesSequenceIDs,
					}
				}),
				emit(func(this *scpb.ViewQuery) *scop.UpdateViewBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateViewBackReferencesInTypes{
						ViewID:  this.ViewID,
						TypeIDs: this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.ViewQuery) *scop.UpdateViewBackReferencesInRoutines {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.UpdateViewBackReferencesInRoutines{
						ViewID:      this.ViewID,
						FunctionIDs: this.UsesFunctionIDs,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ViewQuery) *scop.RemoveBackReferenceInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferenceInTypes{
						BackReferencedDescriptorID: this.ViewID,
						TypeIDs:                    this.UsesTypeIDs,
					}
				}),
				emit(func(this *scpb.ViewQuery) *scop.RemoveBackReferencesInRelations {
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
						BackReferencedID: this.ViewID,
						RelationIDs:      relationIDs,
					}
				}),
				emit(func(this *scpb.ViewQuery) *scop.RemoveBackReferenceInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.RemoveBackReferenceInFunctions{
						BackReferencedDescriptorID: this.ViewID,
						FunctionIDs:                append([]descpb.ID(nil), this.UsesFunctionIDs...),
					}
				}),
			),
		),
	)
}
