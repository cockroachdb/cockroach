// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// ConstantWithMetamorphicTestValue should be used to initialize "magic constants" that
// should be varied during test scenarios to check for bugs at boundary
// conditions. When built with the test_constants build tag, the test value
// will be used. In all other cases, the production value will be used.
// The constant must be a "metamorphic variable": changing it cannot effect the
// output of any SQL DMLs. It can only effect the way in which the data is
// retrieved, because otherwise the main test corpus would fail if this flag
// is enabled.
//
// An example of a "magic constant" that behaves this way is a batch size. Batch
// sizes tend to present testing problems, because often the logic that deals
// with what to do when a batch is finished is less likely to be exercised by
// simple unit tests that don't use enough data to fill up a batch.
//
// For example, instead of writing:
//
// const batchSize = 64
//
// you should write:
//
// var batchSize = util.ConstantWithMetamorphicTestValue(64, 1)
//
// This will give your code a batch size of 1 in the test_constants build
// configuration, increasing the amount of exercise the edge conditions get.
func ConstantWithMetamorphicTestValue(defaultValue, metamorphicValue int) int {
	if metamorphicBuild {
		logMetamorphicValue(metamorphicValue)
		return metamorphicValue
	}
	return defaultValue
}

// ConstantWithMetamorphicTestRange is like ConstantWithMetamorphicTestValue
// except instead of returning a single metamorphic test value, it returns a
// random test value in a range.
func ConstantWithMetamorphicTestRange(defaultValue, min, max int) int {
	if metamorphicBuild {
		ret := int(rand.Int31())%(max-min) + min
		logMetamorphicValue(ret)
		return ret
	}
	return defaultValue
}

func logMetamorphicValue(value int) {
	log.Infof(context.Background(), "initialized metamorphic constant with value %d: %s",
		value, GetSmallTrace(1))
}
