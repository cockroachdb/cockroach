// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsplitter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
)

var _ spanconfig.Splitter = &IllegalSplitter{}

// IllegalSplitter is a Splitter that only returns "illegal use" errors.
type IllegalSplitter struct{}

// Splits is part of spanconfig.Splitter.
func (i IllegalSplitter) Splits(context.Context, catalog.TableDescriptor) (int, error) {
	return 0, errors.New("illegal use of splitter")
}
