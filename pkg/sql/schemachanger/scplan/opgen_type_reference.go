// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/opgen"
)

func init() {
	opGenRegistry.Register(
		(*scpb.TypeReference)(nil),
		scpb.Target_DROP,
		scpb.Status_PUBLIC,
		opgen.To(scpb.Status_ABSENT,
			opgen.Emit(func(this *scpb.TypeReference) scop.Op {
				return &scop.RemoveTypeBackRef{
					TypeID: this.TypeID,
					DescID: this.DescID,
				}
			})),
	)

	opGenRegistry.Register(
		(*scpb.TypeReference)(nil),
		scpb.Target_ADD,
		scpb.Status_ABSENT,
		opgen.To(scpb.Status_PUBLIC,
			opgen.Emit(func(this *scpb.TypeReference) scop.Op {
				return &scop.AddTypeBackRef{
					TypeID: this.TypeID,
					DescID: this.DescID,
				}
			})),
	)
}
