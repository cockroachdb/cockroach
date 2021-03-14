// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import (
	"context"
	"fmt"
	"log"

	"github.com/cockroachdb/errors"
)

func writeLog(ctx context.Context, l string) {
	log.Printf("%s: %s%s", RequestID(ctx), DebuggingPrefix(ctx), l)
}

func writeLogf(ctx context.Context, l string, f ...interface{}) {
	log.Printf("%s: %s%s", RequestID(ctx), DebuggingPrefix(ctx), fmt.Sprintf(l, f...))
}

func wrap(ctx context.Context, err error, wrap string) error {
	return errors.Newf(
		"%s: %s%s: %s",
		RequestID(ctx),
		DebuggingPrefix(ctx),
		wrap,
		err.Error(),
	)
}

func wrapf(ctx context.Context, err error, wrap string, f ...interface{}) error {
	return errors.Newf(
		"%s: %s%s: %s",
		RequestID(ctx),
		DebuggingPrefix(ctx),
		fmt.Sprintf(wrap, f...),
		err.Error(),
	)
}
