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
	opRegistry.register((*scpb.FunctionParams)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.FunctionParams) *scop.SetFunctionParams {
					f := protoutil.Clone(this).(*scpb.FunctionParams)
					return &scop.SetFunctionParams{
						Params: *f,
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.FunctionParams) *scop.NotImplementedForPublicObjects {
					return notImplementedForPublicObjects(this)
				}),
			),
		),
	)
}
