// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

func init() {
	_ = recover()

	_ = fmt.Errorf(wrappedErr.Error())              // want `err.Error\(\) is passed to fmt.Errorf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = fmt.Errorf("format %s", wrappedErr.Error()) // want `err.Error\(\) is passed to fmt.Errorf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`

	s := wrappedErr.Error()
	_ = fmt.Errorf("format %s", s) // this way is allowed

	_ = pgerror.Wrap(anotherErr, pgcode.Warning, wrappedErr.Error())                           // want `err.Error\(\) is passed to pgerror.Wrap; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.Wrapf(anotherErr, pgcode.Warning, "format %s", wrappedErr.Error())             // want `err.Error\(\) is passed to pgerror.Wrapf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.WrapWithDepthf(1, anotherErr, pgcode.Warning, "format %s", wrappedErr.Error()) // want `err.Error\(\) is passed to pgerror.WrapWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.New(pgcode.Warning, wrappedErr.Error())                                        // want `err.Error\(\) is passed to pgerror.New; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.Newf(pgcode.Warning, "format %s", wrappedErr.Error())                          // want `err.Error\(\) is passed to pgerror.Newf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`

	_ = errors.Wrap(anotherErr, wrappedErr.Error())                                          // want `err.Error\(\) is passed to errors.Wrap; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.Wrapf(anotherErr, "format %s", wrappedErr.Error())                            // want `err.Error\(\) is passed to errors.Wrapf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.WrapWithDepthf(1, anotherErr, "format %s", wrappedErr.Error())                // want `err.Error\(\) is passed to errors.WrapWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.New(wrappedErr.Error())                                                       // want `err.Error\(\) is passed to errors.New; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.Newf("format %d %s", 1, wrappedErr.Error())                                   // want `err.Error\(\) is passed to errors.Newf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.NewWithDepthf(1, "format %s", wrappedErr.Error())                             // want `err.Error\(\) is passed to errors.NewWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.AssertionFailedf(wrappedErr.Error())                                          // want `err.Error\(\) is passed to errors.AssertionFailedf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.AssertionFailedWithDepthf(1, "format %s", wrappedErr.Error())                 // want `err.Error\(\) is passed to errors.AssertionFailedWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.NewAssertionErrorWithWrappedErrf(anotherErr, "format %s", wrappedErr.Error()) // want `err.Error\(\) is passed to errors.NewAssertionErrorWithWrappedErrf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`

	_ = fmt.Errorf("got %s", wrappedErr)  // want `non-wrapped error is passed to fmt.Errorf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = fmt.Errorf("got %v", wrappedErr)  // want `non-wrapped error is passed to fmt.Errorf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = fmt.Errorf("got %+v", wrappedErr) // want `non-wrapped error is passed to fmt.Errorf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = fmt.Errorf("got %w", wrappedErr)  // this is allowed because of the %w verb`

	_ = pgerror.Wrapf(anotherErr, pgcode.Warning, "format %s", wrappedErr)             // want `non-wrapped error is passed to pgerror.Wrapf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.WrapWithDepthf(1, anotherErr, pgcode.Warning, "format %s", wrappedErr) // want `non-wrapped error is passed to pgerror.WrapWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = pgerror.Newf(pgcode.Warning, "format %s", wrappedErr)                          // want `non-wrapped error is passed to pgerror.Newf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`

	_ = errors.Wrapf(anotherErr, "format %v", wrappedErr)                            // want `non-wrapped error is passed to errors.Wrapf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.WrapWithDepthf(1, anotherErr, "format %+v", wrappedErr)               // want `non-wrapped error is passed to errors.WrapWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.Newf("format %d %s", 1, wrappedErr)                                   // want `non-wrapped error is passed to errors.Newf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.NewWithDepthf(1, "format %s", wrappedErr)                             // want `non-wrapped error is passed to errors.NewWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.AssertionFailedf("format %v", wrappedErr)                             // want `non-wrapped error is passed to errors.AssertionFailedf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.AssertionFailedWithDepthf(1, "format %s", wrappedErr)                 // want `non-wrapped error is passed to errors.AssertionFailedWithDepthf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`
	_ = errors.NewAssertionErrorWithWrappedErrf(anotherErr, "format %v", wrappedErr) // want `non-wrapped error is passed to errors.NewAssertionErrorWithWrappedErrf; use pgerror.Wrap/errors.Wrap/errors.CombineErrors/errors.WithSecondaryError/errors.NewAssertionErrorWithWrappedErrf instead`

	_ = fmt.Errorf("got %s", wrappedErr)  /* nolint:errwrap */
	_ = fmt.Errorf("got %v", wrappedErr)  /* nolint:errwrap */
	_ = fmt.Errorf("got %+v", wrappedErr) // nolint:errwrap

	// nolint:errwrap
	_ = pgerror.Wrapf(anotherErr, pgcode.Warning, "format %s", wrappedErr)
	// nolint:errwrap
	_ = pgerror.WrapWithDepthf(1, anotherErr, pgcode.Warning, "format %s", wrappedErr)
	// nolint:errwrap
	_ = pgerror.Newf(pgcode.Warning, "format %s", wrappedErr)

	_ = errors.Wrapf(anotherErr, "format %v", wrappedErr)                            /* nolint:errwrap */
	_ = errors.WrapWithDepthf(1, anotherErr, "format %+v", wrappedErr)               /* nolint:errwrap */
	_ = errors.Newf("format %d %s", 1, wrappedErr)                                   /* nolint:errwrap */
	_ = errors.NewWithDepthf(1, "format %s", wrappedErr)                             /* nolint:errwrap */
	_ = errors.AssertionFailedf("format %v", wrappedErr)                             /* nolint:errwrap */
	_ = errors.AssertionFailedWithDepthf(1, "format %s", wrappedErr)                 /* nolint:errwrap */
	_ = errors.NewAssertionErrorWithWrappedErrf(anotherErr, "format %v", wrappedErr) /* nolint:errwrap */

}
