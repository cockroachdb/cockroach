// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigbounds

// ForEachField enumerates the fields.
func ForEachField(fn func(Field)) {
	for _, field := range fields {
		fn(field)
	}
}
