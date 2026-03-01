// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import "time"

const (
	// Options defaults
	DefaultCleanupInterval          = 24 * time.Hour
	DefaultExpiredTokensRetention   = 24 * time.Hour
	DefaultStatisticsUpdateInterval = 30 * time.Second
	DefaultTokenLastUsedBufferSize  = 256

	// Token limits - enforced in service layer
	MaxTokensPerServiceAccount = 10

	TokenPrefix        = "rp"
	TokenTypeUser      = "user"
	TokenTypeSA        = "sa"
	TokenVersion       = "1"
	TokenEntropyLength = 43

	// Token TTL constraints
	MaxTokenTTL                            = 365 * 24 * time.Hour // Nothing over 1 year
	TokenDefaultTTLUser                    = 30 * 24 * time.Hour  // 30 days (users have to re-authenticate periodically)
	TokenDefaultTTLServiceAccount          = 365 * 24 * time.Hour // 1 year (service accounts are long-lived)
	TokenDefaultTTLBootstrapServiceAccount = 6 * time.Hour        // 6 hours (bootstrap accounts are short-lived
)
