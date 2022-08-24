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
				emit(func(this *scpb.Column) *scop.MakeAddedColumnDeleteOnly {
					return &scop.MakeAddedColumnDeleteOnly{
						Column: *protoutil.Clone(this).(*scpb.Column),
					}
				}),
				emit(func(this *scpb.Column, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
			),
			to(scpb.Status_WRITE_ONLY,
				emit(func(this *scpb.Column) *scop.MakeAddedColumnDeleteAndWriteOnly {
					return &scop.MakeAddedColumnDeleteAndWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.Column, md *targetsWithElementMap) *scop.MakeColumnPublic {
					return &scop.MakeColumnPublic{
						EventBase: newLogEventBase(this, md),
						TableID:   this.TableID,
						ColumnID:  this.ColumnID,
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
				emit(func(this *scpb.Column) *scop.MakeDroppedColumnDeleteAndWriteOnly {
					return &scop.MakeDroppedColumnDeleteAndWriteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
				emit(func(this *scpb.Column, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
			),
			to(scpb.Status_DELETE_ONLY,
				revertible(false),
				emit(func(this *scpb.Column) *scop.MakeDroppedColumnDeleteOnly {
					return &scop.MakeDroppedColumnDeleteOnly{
						TableID:  this.TableID,
						ColumnID: this.ColumnID,
					}
				}),
			),
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.Column, md *targetsWithElementMap) *scop.MakeColumnAbsent {
					return &scop.MakeColumnAbsent{
						EventBase: newLogEventBase(this, md),
						TableID:   this.TableID,
						ColumnID:  this.ColumnID,
					}
				}),
			),
		),
	)
}
