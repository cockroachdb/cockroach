// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catformat_test

import "github.com/cockroachdb/cockroach/pkg/sql"

func init() {
	sql.DoParserInjection()
}
