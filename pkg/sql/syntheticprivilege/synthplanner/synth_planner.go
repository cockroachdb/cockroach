// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package synthplanner contains the interface SyntheticPrivilegeNeededPlanner,
// which contains functions needed by the GetPrivilegeDescriptor() function
// in syntheticprivilege package.
package synthplanner

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
)

// SyntheticPrivilegeNeededPlanner implements parts of the evalinterfaces.Planner
// interfaces.
type SyntheticPrivilegeNeededPlanner interface {
	// IsActive returns if the version specified by key is active.
	IsActive(ctx context.Context, key clusterversion.Key) bool

	// SynthesizePrivilegeDescriptor synthesizes a
	// PrivilegeDescriptor given a SyntheticPrivilegeObject's path
	// from system.privileges.
	SynthesizePrivilegeDescriptor(
		ctx context.Context,
		privilegeObjectPath string,
		privilegeObjectType privilege.ObjectType,
	) (*catpb.PrivilegeDescriptor, error)
}
