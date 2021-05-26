// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
)

// Sink provides clients with interfaces to send statistics data into the sink.
type Sink interface {
	// AddAppStats ingests a single ssmemstorage.Container for a given appName.
	AddAppStats(ctx context.Context, appName string, other *ssmemstorage.Container) error
}

var _ Sink = &SQLStats{}

// AddAppStats implements the Sink interface.
func (s *SQLStats) AddAppStats(
	ctx context.Context, appName string, other *ssmemstorage.Container,
) error {
	stats := s.getStatsForApplication(appName)
	// Container.Add() manages locks for itself, so we don't need to guard it
	// with locks.
	return stats.Add(ctx, other)
}
