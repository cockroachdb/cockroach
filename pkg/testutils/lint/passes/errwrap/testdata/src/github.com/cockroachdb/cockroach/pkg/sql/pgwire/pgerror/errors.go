// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgerror

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
)

func NewWithDepthf(depth int, code pgcode.Code, format string, args ...interface{}) error {
	return fmt.Errorf(format, args)
}

func New(code pgcode.Code, msg string) error {
	return fmt.Errorf(msg)

}

func Newf(code pgcode.Code, format string, args ...interface{}) error {
	return fmt.Errorf(format, args)
}

func Wrapf(err error, code pgcode.Code, format string, args ...interface{}) error {
	return fmt.Errorf(format, args)
}

func WrapWithDepthf(
	depth int, err error, code pgcode.Code, format string, args ...interface{},
) error {
	return fmt.Errorf(format, args)

}

func Wrap(err error, code pgcode.Code, msg string) error {
	return fmt.Errorf(msg)
}
