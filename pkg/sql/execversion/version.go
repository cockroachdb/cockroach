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

// V identifies DistSQL engine versions. It determines the execution logic to be
// used for a particular DistSQL flow and is picked by the gateway node after
// consulting the cluster version.
type V uint32

// V24_3 is the exec version of all binaries of 24.3 and prior cockroach
// versions.
const V24_3 = V(71)

// V25_1 is the exec version of all binaries of 25.1 cockroach versions. It can
// only be used by the flows once the cluster has upgraded to 25.1.
const V25_1 = V(72)

// V25_2 is the exec version of all binaries of 25.2 cockroach versions. It can
// only be used by the flows once the cluster has upgraded to 25.2.
const V25_2 = V(73)

// MinAccepted is the oldest version that the server is compatible with. A
// server will not accept flows with older versions.
const MinAccepted = V24_3

// Latest is the latest exec version supported by this binary.
const Latest = V25_2

var contextVersionKey = ctxutil.RegisterFastValueKey()

// WithVersion returns the updated context that stores the given version.
func WithVersion(ctx context.Context, version V) context.Context {
	return ctxutil.WithFastValue(ctx, contextVersionKey, version)
}

// TestingWithLatestCtx is a context that has the latest exec version installed.
// It should only be used in tests.
var TestingWithLatestCtx = WithVersion(context.Background(), Latest)

// FromContext returns the version stored in the context. It panics if the
// version is not found.
func FromContext(ctx context.Context) V {
	val := ctxutil.FastValue(ctx, contextVersionKey)
	if v, ok := val.(V); !ok {
		panic(errors.AssertionFailedf("didn't find execversion in context.Context"))
	} else {
		return v
	}
}
