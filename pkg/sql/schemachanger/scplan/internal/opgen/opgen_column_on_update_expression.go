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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	opRegistry.register((*scpb.ColumnOnUpdateExpression)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					return &scop.AddColumnOnUpdateExpression{
						OnUpdate: *protoutil.Clone(this).(*scpb.ColumnOnUpdateExpression),
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					if ids := catalog.MakeDescriptorIDSet(this.UsesTypeIDs...); !ids.Empty() {
						return &scop.UpdateBackReferencesInTypes{
							TypeIDs:              ids,
							BackReferencedDescID: this.TableID,
						}
					}
					return nil
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					return &scop.UpdateBackReferencesInSequences{
						SequenceIDs:            catalog.MakeDescriptorIDSet(this.UsesSequenceIDs...),
						BackReferencedTableID:  this.TableID,
						BackReferencedColumnID: this.ColumnID,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					return &scop.RemoveColumnOnUpdateExpression{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					if ids := catalog.MakeDescriptorIDSet(this.UsesTypeIDs...); !ids.Empty() {
						return &scop.UpdateBackReferencesInTypes{
							TypeIDs:              ids,
							BackReferencedDescID: this.TableID,
						}
					}
					return nil
				}),
				emit(func(this *scpb.ColumnOnUpdateExpression) scop.Op {
					if ids := catalog.MakeDescriptorIDSet(this.UsesSequenceIDs...); !ids.Empty() {
						return &scop.UpdateBackReferencesInSequences{
							SequenceIDs:            ids,
							BackReferencedTableID:  this.TableID,
							BackReferencedColumnID: this.ColumnID,
						}
					}
					return nil
				}),
			),
		),
	)
}
