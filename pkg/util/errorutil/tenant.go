// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package errorutil

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// UnsupportedUnderClusterVirtualizationMessage is the message used by UnsupportedUnderClusterVirtualization error.
const UnsupportedUnderClusterVirtualizationMessage = "operation is unsupported within a virtual cluster"

// UnsupportedUnderClusterVirtualization returns an error suitable for
// returning when an operation could not be carried out due to the SQL
// server running inside a virtual cluster. In that mode, Gossip and
// other components of the KV layer are not available.
func UnsupportedUnderClusterVirtualization() error {
	return pgerror.New(pgcode.FeatureNotSupported, UnsupportedUnderClusterVirtualizationMessage)
}
