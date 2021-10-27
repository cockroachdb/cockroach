// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package slbase provides definitions shared by all subpackages of the
// sqlliveness subsystem.
package slbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestingKnobs contains test knobs for sqlliveness system behavior.
type TestingKnobs struct {
	// SessionOverride is used to override the returned session.
	// If it returns nil, nil the underlying instance will be used.
	SessionOverride func(ctx context.Context) (sqlliveness.Session, error)
	// NewTimer is used to override the construction of new timers.
	NewTimer func() timeutil.TimerI
	// SQLLivenessTableID overrides the table used to store liveness records.
	// The default is keys.SqllivenessID.
	SQLLivenessTableID descpb.ID
}

var _ base.ModuleTestingKnobs = &TestingKnobs{}

// ModuleTestingKnobs implements the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
