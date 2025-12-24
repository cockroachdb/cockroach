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
	opRegistry.register((*scpb.TableStorageParam)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.TableStorageParam) *scop.SetTableStorageParam {
					return &scop.SetTableStorageParam{
						Param: *protoutil.Clone(this).(*scpb.TableStorageParam),
					}
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.TableStorageParam) *scop.ResetTableStorageParam {
					return &scop.ResetTableStorageParam{
						Param: *protoutil.Clone(this).(*scpb.TableStorageParam),
					}
				}),
			),
		),
	)
}
