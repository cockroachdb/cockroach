// Copyright 2024 The Cockroach Authors.
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
	opRegistry.register((*scpb.TriggerDeps)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TriggerDeps) *scop.SetTriggerForwardReferences {
					if len(this.UsesRelationIDs) == 0 && len(this.UsesTypeIDs) == 0 &&
						len(this.UsesRoutineIDs) == 0 {
						return nil
					}
					return &scop.SetTriggerForwardReferences{Deps: *protoutil.Clone(this).(*scpb.TriggerDeps)}
				}),
				emit(func(this *scpb.TriggerDeps) *scop.UpdateTableBackReferencesInRelations {
					if len(this.UsesRelationIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInRelations{
						TableID:     this.TableID,
						RelationIDs: this.UsesRelationIDs,
					}
				}),
				emit(func(this *scpb.TriggerDeps) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.TriggerDeps) *scop.AddTriggerBackReferencesInRoutines {
					if len(this.UsesRoutineIDs) == 0 {
						return nil
					}
					return &scop.AddTriggerBackReferencesInRoutines{
						RoutineIDs:              this.UsesRoutineIDs,
						BackReferencedTableID:   this.TableID,
						BackReferencedTriggerID: this.TriggerID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TriggerDeps) *scop.UpdateTableBackReferencesInRelations {
					if len(this.UsesRelationIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInRelations{
						TableID:     this.TableID,
						RelationIDs: this.UsesRelationIDs,
					}
				}),
				emit(func(this *scpb.TriggerDeps) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.TriggerDeps) *scop.RemoveTriggerBackReferencesInRoutines {
					if len(this.UsesRoutineIDs) == 0 {
						return nil
					}
					return &scop.RemoveTriggerBackReferencesInRoutines{
						RoutineIDs:              this.UsesRoutineIDs,
						BackReferencedTableID:   this.TableID,
						BackReferencedTriggerID: this.TriggerID,
					}
				}),
			),
		),
	)
}
