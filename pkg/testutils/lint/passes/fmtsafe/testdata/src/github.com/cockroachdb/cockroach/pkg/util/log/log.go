// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import (
	"context"
	"fmt"
)

func Errorf(ctx context.Context, format string, args ...interface{}) {
	fmt.Println(fmt.Sprintf(format, args...))
}

func Error(ctx context.Context, msg string) {
	fmt.Println(msg)
}
