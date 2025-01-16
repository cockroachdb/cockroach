// Copyright 2025 The Cockroach Authors.
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
	opRegistry.register((*scpb.PolicyDeps)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.PolicyDeps) *scop.SetPolicyForwardReferences {
					// Note: Even if the dependencies are empty, we still set the policy's
					// forward references. This ensures that any old forward dependencies
					// are properly cleared.
					return &scop.SetPolicyForwardReferences{Deps: *protoutil.Clone(this).(*scpb.PolicyDeps)}
				}),
				emit(func(this *scpb.PolicyDeps) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.PolicyDeps) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesRelationIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.UsesRelationIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.PolicyDeps) *scop.AddPolicyBackReferenceInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.AddPolicyBackReferenceInFunctions{
						BackReferencedTableID:  this.TableID,
						BackReferencedPolicyID: this.PolicyID,
						FunctionIDs:            this.UsesFunctionIDs,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.PolicyDeps) *scop.UpdateTableBackReferencesInTypes {
					if len(this.UsesTypeIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInTypes{
						TypeIDs:               this.UsesTypeIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.PolicyDeps) *scop.UpdateTableBackReferencesInSequences {
					if len(this.UsesRelationIDs) == 0 {
						return nil
					}
					return &scop.UpdateTableBackReferencesInSequences{
						SequenceIDs:           this.UsesRelationIDs,
						BackReferencedTableID: this.TableID,
					}
				}),
				emit(func(this *scpb.PolicyDeps) *scop.RemovePolicyBackReferenceInFunctions {
					if len(this.UsesFunctionIDs) == 0 {
						return nil
					}
					return &scop.RemovePolicyBackReferenceInFunctions{
						BackReferencedTableID:  this.TableID,
						BackReferencedPolicyID: this.PolicyID,
						FunctionIDs:            this.UsesFunctionIDs,
					}
				}),
			),
		),
	)
}
