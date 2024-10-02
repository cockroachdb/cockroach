// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfig

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// Record ties a target to its corresponding config.
type Record struct {
	// target specifies the target (keyspan(s)) the config applies over.
	target Target

	// config is the set of attributes that apply over the corresponding target.
	config roachpb.SpanConfig
}

// MakeRecord returns a Record with the specified Target and SpanConfig. If the
// Record targets a SystemTarget, we also validate the SpanConfig.
func MakeRecord(target Target, cfg roachpb.SpanConfig) (Record, error) {
	if target.IsSystemTarget() {
		if err := cfg.ValidateSystemTargetSpanConfig(); err != nil {
			return Record{},
				errors.NewAssertionErrorWithWrappedErrf(err, "failed to validate SystemTarget SpanConfig")
		}
	}
	return Record{target: target, config: cfg}, nil
}

// IsEmpty returns true if the receiver is an empty Record.
func (r *Record) IsEmpty() bool {
	return r.target.isEmpty() && r.config.IsEmpty()
}

// GetTarget returns the Record target.
func (r *Record) GetTarget() Target {
	return r.target
}

// GetConfig returns the Record config.
func (r *Record) GetConfig() roachpb.SpanConfig {
	return r.config
}
