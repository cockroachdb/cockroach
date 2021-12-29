// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

var alterColAfterNotSupportedErr = unimplemented.NewWithIssuef(
	66122, "ALTER COLUMN AFTER is currently not supported")

func AlterColumnAfter() error {
	return alterColAfterNotSupportedErr
}
