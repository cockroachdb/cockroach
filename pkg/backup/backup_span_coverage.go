// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// checkCoverage verifies that spans are covered by a given chain of backups.
func checkCoverage(
	ctx context.Context, spans []roachpb.Span, backups []backuppb.BackupManifest,
) error {
	if len(spans) == 0 {
		return nil
	}

	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return err
	}
	defer frontier.Release()

	// The main loop below requires the entire frontier be caught up to the start
	// time of each step it proceeds, however a span introduced in a later backup
	// would hold back the whole frontier at 0 until it is reached, so run through
	// all layers first to unconditionally advance the introduced spans.
	for i := range backups {
		for _, sp := range backups[i].IntroducedSpans {
			if _, err := frontier.Forward(sp, backups[i].StartTime); err != nil {
				return err
			}
		}
	}

	// Walk through the chain of backups in order advancing the spans each covers
	// and verify that the entire required span frontier is covered as expected.
	for i := range backups {
		// This backup advances its covered spans _from its start time_ to its end
		// time, so before actually advance those spans in the frontier to that end
		// time, assert that it is starting at the start time, i.e. that this
		// backup does indeed pick up where the prior backup left off.
		if start, required := frontier.Frontier(), backups[i].StartTime; start.Less(required) {
			s := frontier.PeekFrontierSpan()
			return errors.Errorf(
				"no backup covers time [%s,%s) for range [%s,%s) (or backups listed out of order)",
				start, required, s.Key, s.EndKey,
			)
		}

		// Advance every span the backup covers to its end time.
		for _, s := range backups[i].Spans {
			if _, err := frontier.Forward(s, backups[i].EndTime); err != nil {
				return err
			}
		}

		// Check that the backup actually covered all the required spans.
		if end, required := frontier.Frontier(), backups[i].EndTime; end.Less(required) {
			return errors.Errorf("expected previous backups to cover until time %v, got %v (e.g. span %v)",
				required, end, frontier.PeekFrontierSpan())
		}
	}

	return nil
}
