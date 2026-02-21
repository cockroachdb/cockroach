// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package metrics

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultStatisticsUpdateInterval is how often gauge metrics are updated.
	DefaultStatisticsUpdateInterval = 30 * time.Second
)

// ICollector defines the interface for updating gauge metrics.
type ICollector interface {
	// UpdateMetrics fetches statistics and updates gauges.
	UpdateMetrics(ctx context.Context, l *logger.Logger) error
}

// StartMetricsCollection starts a goroutine that periodically updates gauge metrics.
func StartMetricsCollection(
	ctx context.Context,
	l *logger.Logger,
	errChan chan<- error,
	updateInterval time.Duration,
	collector ICollector,
	onComplete func(),
) error {
	l.Debug("Starting auth statistics routine")

	go func() {
		defer func() {
			l.Debug("Auth statistics routine stopped")
			if onComplete != nil {
				onComplete()
			}
		}()

		ticker := time.NewTicker(updateInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				l.Debug("Stopping auth statistics routine")
				return

			case <-ticker.C:
				l.Debug("Getting auth statistics from the repository")
				if err := collector.UpdateMetrics(ctx, l); err != nil {
					errChan <- errors.Wrap(err, "unable to get auth statistics")
				}
			}
		}
	}()

	return nil
}
