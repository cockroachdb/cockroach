// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/colexec"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// isAggregateSupported checks whether the aggregate function that operates on
// columns of types 'inputTypes' (which can be empty in case of COUNT_ROWS) is
// supported and returns an error if it isn't.
func isAggregateSupported(aggFn execinfrapb.AggregatorSpec_Func, inputTypes []*types.T) error {
	supported := false
	for _, supportedAggFn := range colexec.SupportedAggFns {
		if aggFn == supportedAggFn {
			supported = true
			break
		}
	}
	if !supported {
		return errors.Errorf("unsupported columnar aggregate function %s", aggFn.String())
	}
	_, err := colexec.MakeAggregateFuncsOutputTypes(
		[][]*types.T{inputTypes},
		[]execinfrapb.AggregatorSpec_Func{aggFn},
	)
	return err
}
