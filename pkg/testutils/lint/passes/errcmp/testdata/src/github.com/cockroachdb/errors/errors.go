// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errors

import "errors"

// New constructs a new error.
func New(s string) error {
	return errors.New(s)
}

// Is is a shim for testing.
func Is(err, referece error) bool {
	return false
}
