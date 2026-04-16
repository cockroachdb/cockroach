// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

// ScanOption configures the behavior of Scan and ReverseScan operations.
type ScanOption func(*scanConfig)

type scanConfig struct {
	targetBytes int64
}

// WithTargetBytes sets a byte limit on the scan response. When non-zero, the
// scan will stop after the cumulative size of returned keys and values exceeds
// this threshold. At least one key-value pair is always returned regardless of
// the limit. When combined with maxRows, both limits apply and the scan stops
// as soon as either is reached. A ResumeSpan is returned in the Result when
// the byte limit causes early termination.
func WithTargetBytes(targetBytes int64) ScanOption {
	return func(c *scanConfig) {
		c.targetBytes = targetBytes
	}
}

func buildScanConfig(opts []ScanOption) scanConfig {
	var cfg scanConfig
	for _, o := range opts {
		o(&cfg)
	}
	return cfg
}
