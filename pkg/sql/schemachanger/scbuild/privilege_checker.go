// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/internal/scbuildstmts"
)

var _ scbuildstmts.PrivilegeChecker = buildCtx{}

// MustOwn implements the scbuildstmts.PrivilegeChecker interface.
func (b buildCtx) MustOwn(ctx context.Context, desc catalog.Descriptor) {
	hasAdmin, err := b.AuthorizationAccessor().HasAdminRole(ctx)
	if err != nil {
		panic(err)
	}
	if hasAdmin {
		return
	}
	hasOwnership, err := b.AuthorizationAccessor().HasOwnership(ctx, desc)
	if err != nil {
		panic(err)
	}
	if !hasOwnership {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of %s %q", desc.DescriptorType(), desc.GetName()))
	}
}
