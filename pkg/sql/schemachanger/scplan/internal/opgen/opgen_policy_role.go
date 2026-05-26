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
	opRegistry.register((*scpb.PolicyRole)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.PolicyRole) *scop.AddPolicyRole {
					return &scop.AddPolicyRole{Role: *protoutil.Clone(this).(*scpb.PolicyRole)}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.PolicyRole) *scop.RemovePolicyRole {
					return &scop.RemovePolicyRole{Role: *protoutil.Clone(this).(*scpb.PolicyRole)}
				}),
			),
		),
	)
}
