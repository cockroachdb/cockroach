// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execversion

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
	"github.com/cockroachdb/errors"
)

// DistSQLVersion identifies DistSQL engine versions. It determines the
// execution logic to be used for a particular DistSQL flow and is picked by the
// gateway node after consulting the cluster version.
type DistSQLVersion uint32

// V24_3 is the exec version of all binaries of 24.3 and prior cockroach
// versions.
const V24_3 = DistSQLVersion(71)

// V25_1 is the exec version of all binaries of 25.1 cockroach versions. It can
// only be used by the flows once the cluster has upgraded to 25.1.
const V25_1 = DistSQLVersion(72)

// MinAccepted is the oldest version that the server is compatible with. A
// server will not accept flows with older versions.
const MinAccepted = V24_3

// Latest is the latest exec version supported by this binary.
const Latest = V25_1

var contextVersionKey = ctxutil.RegisterFastValueKey()

// WithVersion returns the updated context that stores the given version.
func WithVersion(ctx context.Context, version DistSQLVersion) context.Context {
	return ctxutil.WithFastValue(ctx, contextVersionKey, version)
}

// FromContext returns the version stored in the context. It panics if the
// version is not found.
func FromContext(ctx context.Context) DistSQLVersion {
	val := ctxutil.FastValue(ctx, contextVersionKey)
	if v, ok := val.(DistSQLVersion); !ok {
		panic(errors.AssertionFailedf("didn't find execversion in context.Context"))
	} else {
		return v
	}
}

// Silence the unused linter for now.
var _ = FromContext
