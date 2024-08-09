// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) AddTableZoneConfig(
	ctx context.Context, op scop.AddTableZoneConfig,
) error {
	i.ImmediateMutationStateUpdater.UpdateZoneConfig(op.TableID, op.ZoneConfig)
	return nil
}
