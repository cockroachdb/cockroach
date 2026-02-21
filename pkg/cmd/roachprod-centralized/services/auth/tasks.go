// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func (s *Service) CleanupRevokedAndExpiredTokens(
	ctx context.Context, l *logger.Logger, retention time.Duration,
) (int, error) {

	totalCleanedUp := 0

	for _, status := range []auth.TokenStatus{auth.TokenStatusValid, auth.TokenStatusRevoked} {
		start := timeutil.Now()
		count, err := s.repo.CleanupTokens(ctx, l, status, retention)
		latency := timeutil.Since(start)

		if err != nil {
			s.auditEvent(ctx, l, nil, AuditTokenCleanedUp, "error", map[string]interface{}{
				"routine":      "CleanupRevokedAndExpiredTokens",
				"token_status": status,
				"retention":    retention.String(),
				"error":        err.Error(),
			})

			return 0, errors.Wrapf(err, "failed to cleanup %s tokens", status)
		}

		s.auditEvent(ctx, l, nil, AuditTokenCleanedUp, "success", map[string]interface{}{
			"routine":        "CleanupRevokedAndExpiredTokens",
			"token_status":   status,
			"retention":      retention.String(),
			"tokens_deleted": count,
		})

		// Record cleanup metrics
		s.metrics.RecordTokenCleanup(string(status), count, latency)

		totalCleanedUp += count
	}

	return totalCleanedUp, nil
}
