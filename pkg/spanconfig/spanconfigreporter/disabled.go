// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigreporter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/errors"
)

// DisabledReporter is a spanconfig.Reporter that only returns "disabled"
// errors.
var DisabledReporter = disabled{}

type disabled struct{}

var _ spanconfig.Reporter = disabled{}

// SpanConfigConformance implements the spanconfig.Reporter interface.
func (d disabled) SpanConfigConformance(
	ctx context.Context, spans []roachpb.Span,
) (roachpb.SpanConfigConformanceReport, error) {
	return roachpb.SpanConfigConformanceReport{}, errors.New("span configs disabled")
}
