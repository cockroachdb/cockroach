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

// ConstantWithTestValue should be used to initialize "magic constants" that
// should be varied during test scenarios to check for bugs at boundary
// conditions. When built with the test_constants build tag, the test value
// will be used. In all other cases, the production value will be used.
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
// var batchSize = util.ConstantWithTestValue(64, 1)
//
// This will give your code a batch size of 1 in the test_constants build
// configuration, increasing the amount of exercise the edge conditions get.
func ConstantWithTestValue(production, test int) int {
	if TestConstants {
		return test
	}
	return production
}
