// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package timeutil contains time utility functions.

Note that this package contains the following initialization.

	func init() {
		time.Local = time.UTC
	}

Setting the time.Local global makes this package unfriendly to being used
outside CRDB.
*/
package timeutil
