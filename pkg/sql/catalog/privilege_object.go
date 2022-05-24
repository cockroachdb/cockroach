// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// PrivilegeObject represents an object that can have privileges. The privileges
// can either live on the descriptor or in the system.privileges table.
type PrivilegeObject interface {
	GetPrivilegeDescriptor(ctx context.Context, planner eval.Planner) (*catpb.PrivilegeDescriptor, error)
	GetObjectType() string
	GetName() string
}
