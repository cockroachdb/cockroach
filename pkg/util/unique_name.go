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

import "fmt"

// GenerateUniqueConstraintName attempts to generate a unique constraint name
// with the given prefix.
// It will first try prefix by itself, then it will subsequently try
// adding numeric digits at the end, starting from 1.
func GenerateUniqueConstraintName(prefix string, nameExistsFunc func(name string) bool) string {
	name := prefix
	for i := 1; nameExistsFunc(name); i++ {
		name = fmt.Sprintf("%s_%d", prefix, i)
	}
	return name
}
