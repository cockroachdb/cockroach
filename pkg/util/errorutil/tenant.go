// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package errorutil

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// UnsupportedWithMultiTenancy returns an error suitable for returning when an
// operation could not be carried out due to the SQL server running in
// multi-tenancy mode. In that mode, Gossip and other components of the KV layer
// are not available.
func UnsupportedWithMultiTenancy(issues ...int) error {
	var buf strings.Builder
	buf.WriteString("operation is unsupported in multi-tenancy mode")
	if len(issues) > 0 {
		buf.WriteString("; see:\n")
		const prefix = "\nhttps://github.com/cockroachdb/cockroach/issues/"
		for _, n := range issues {
			fmt.Fprint(&buf, prefix, n)
		}
	}
	// TODO(knz): This should be done in a different way,
	// see: https://github.com/cockroachdb/cockroach/issues/48164
	return errors.Newf("%s", buf.String())
}
