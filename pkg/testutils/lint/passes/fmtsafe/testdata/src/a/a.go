// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3"
)

var unsafeStr = "abc %d"

const constOk = "safe %d"

func init() {
	_ = recover()

	_ = errors.New(unsafeStr) // want `message argument is not a constant expression`

	// Even though the following is trying to opt out of the linter,
	// the opt out fails because the code is not in a test.

	_ = errors.New(unsafeStr /*nolint:fmtsafe*/) // want `message argument is not a constant expression`

	_ = errors.New("safestr")
	_ = errors.New(constOk)
	_ = errors.New("abo" + constOk)
	_ = errors.New("abo" + unsafeStr) // want `message argument is not a constant expression`

	_ = errors.Newf("safe %d", 123)
	_ = errors.Newf(constOk, 123)
	_ = errors.Newf(unsafeStr, 123) // want `format argument is not a constant expression`
	_ = errors.Newf("abo"+constOk, 123)
	_ = errors.Newf("abo"+unsafeStr, 123) // want `format argument is not a constant expression`

	ctx := context.Background()

	log.Errorf(ctx, "safe %d", 123)
	log.Errorf(ctx, constOk, 123)
	log.Errorf(ctx, unsafeStr, 123) // want `format argument is not a constant expression`
	log.Errorf(ctx, "abo"+constOk, 123)
	log.Errorf(ctx, "abo"+unsafeStr, 123) // want `format argument is not a constant expression`

	var m myLogger
	var l raft.Logger = m

	l.Infof("safe %d", 123)
	l.Infof(constOk, 123)
	l.Infof(unsafeStr, 123) // want `format argument is not a constant expression`
	l.Infof("abo"+constOk, 123)
	l.Infof("abo"+unsafeStr, 123) // want `format argument is not a constant expression`
}

type myLogger struct{}

func (m myLogger) Info(args ...interface{}) {
	log.Errorf(context.Background(), "", args...)
}

func (m myLogger) Infof(_ string, args ...interface{}) {
	log.Errorf(context.Background(), "ignoredfmt", args...)
}
