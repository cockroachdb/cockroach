// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigsplitter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

var _ spanconfig.Splitter = &NoopSplitter{}

// NoopSplitter is a Splitter that only returns "illegal use" errors.
type NoopSplitter struct{}

// Splits is part of spanconfig.Splitter.
func (i NoopSplitter) Splits(context.Context, catalog.TableDescriptor) (int, error) {
	return 0, nil
}
