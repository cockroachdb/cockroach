// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.SequenceValue)(nil),
		toTransientAbsent(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.SequenceValue) *scop.MaybeUpdateSequenceValue {
					return &scop.MaybeUpdateSequenceValue{
						SequenceID:       this.SequenceID,
						PrevIncrement:    this.PrevIncrement,
						UpdatedIncrement: this.UpdatedIncrement,
						PrevMinValue:     this.PrevMinValue,
						UpdatedMinValue:  this.UpdatedMinValue,
						PrevMaxValue:     this.PrevMaxValue,
						UpdatedMaxValue:  this.UpdatedMaxValue,
						PrevStart:        this.PrevStart,
						UpdatedStart:     this.UpdatedStart,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT),
		),
	)
}
