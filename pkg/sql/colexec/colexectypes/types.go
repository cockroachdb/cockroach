// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexectypes

import (
	"github.com/cockroachdb/cockroach/pkg/col/phystypes"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// T is an extension of a physical type that additionally contains original SQL
// type.
type T struct {
	phystypes.T

	LogicalType *types.T
}
