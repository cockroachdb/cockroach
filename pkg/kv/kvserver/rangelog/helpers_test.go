// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangelog

// NewTestWriter allows tests to use a custom table.
var NewTestWriter = newWriter
