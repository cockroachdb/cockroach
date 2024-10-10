// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropTrigger is UNIMPLEMENTED for the legacy schema changer.
func (p *planner) DropTrigger(_ context.Context, _ *tree.DropTrigger) (planNode, error) {
	return nil, pgerror.New(pgcode.FeatureNotSupported,
		"DROP TRIGGER is only implemented in the declarative schema changer")
}
