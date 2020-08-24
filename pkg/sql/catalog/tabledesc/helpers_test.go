// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

func (desc *Immutable) ValidateCrossReferences(ctx context.Context, dg catalog.DescGetter) error {
	return desc.validateCrossReferences(ctx, dg)
}

func (desc *Immutable) ValidatePartitioning() error {
	return desc.validatePartitioning()
}

var FitColumnToFamily = fitColumnToFamily
