// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import "github.com/cockroachdb/cockroach/pkg/sql/isql"

type options struct {
	db                      isql.DB
	isSystemTenant          bool
	testingKnobs            *TestingKnobs
	telemetryStatusReporter TelemetryStatusReporter
	metadataAccessor        MetadataAccessor
}

type Option interface {
	apply(*options)
}

type optionFunc func(*options)

func (f optionFunc) apply(o *options) {
	f(o)
}

func WithDB(db isql.DB) Option {
	return optionFunc(func(o *options) {
		o.db = db
	})
}

func WithSystemTenant(v bool) Option {
	return optionFunc(func(o *options) {
		o.isSystemTenant = v
	})
}

func WithTestingKnobs(tk *TestingKnobs) Option {
	return optionFunc(func(o *options) {
		if tk != nil {
			o.testingKnobs = tk
		}
	})
}

func WithTelemetryStatusReporter(r TelemetryStatusReporter) Option {
	return optionFunc(func(o *options) {
		o.telemetryStatusReporter = r
	})
}

func WithMetadataAccessor(m MetadataAccessor) Option {
	return optionFunc(func(o *options) {
		o.metadataAccessor = m
	})
}
