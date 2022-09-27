// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syntheticprivilege

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// PrivilegeSynthesizer is used to synthesize a PrivilegeDescriptor from
// synthetic privileges - privileges that live in system.privileges.
type PrivilegeSynthesizer interface {
	SynthesizePrivilegeDescriptor(
		ctx context.Context, txn *kv.Txn, name string, privilegeObjectType privilege.ObjectType, tableVersion descpb.DescriptorVersion,
	) (*catpb.PrivilegeDescriptor, error)
	SetIEFactory(ieFactory sqlutil.InternalExecutorFactory)
}
