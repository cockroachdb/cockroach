// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigcatalog

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
)

var _ spanconfig.Config = (*SpanConfig)(nil)

// SpanConfig wraps a roachpb.SpanConfig proto and implements the Config
// interface.
type SpanConfig struct {
	roachpb.SpanConfig
}

// SpanConfig implements the Config interface.
func (*SpanConfig) Config() {}

// GenerateSpanConfig implements the Config interface.
func (s *SpanConfig) GenerateSpanConfig() (roachpb.SpanConfig, error) {
	return s.SpanConfig, nil
}

// NewSpanConfigFromProto returns a new SpanConfig.
func NewSpanConfigFromProto(config roachpb.SpanConfig) *SpanConfig {
	return &SpanConfig{
		SpanConfig: config,
	}
}
