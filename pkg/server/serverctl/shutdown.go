// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverctl

import "github.com/cockroachdb/errors"

// ShutdownCause returns the reason for the shutdown request, as an error.
func (r ShutdownRequest) ShutdownCause() error {
	switch r.Reason {
	case ShutdownReasonDrainRPC:
		return errors.Newf("shutdown requested by drain RPC")
	default:
		return r.Err
	}
}

// TerminateUsingGracefulDrain determines whether the shutdown should
// be effected via a graceful drain first.
func (r ShutdownRequest) TerminateUsingGracefulDrain() bool {
	// Note: ShutdownReasonDrainRPC despite its name cannot be effected
	// via a graceful drain, because at the time it is generated
	// the RPC subsystem is shut down already.
	return r.Reason == ShutdownReasonGracefulStopRequestedByOrchestration
}

// Empty returns true if the receiver is the zero value.
func (r ShutdownRequest) Empty() bool {
	return r == (ShutdownRequest{})
}

// MakeShutdownRequest constructs a serverctl.ShutdownRequest.
func MakeShutdownRequest(reason ShutdownReason, err error) ShutdownRequest {
	if reason == ShutdownReasonDrainRPC && err != nil {
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "programming error: unexpected err for ShutdownReasonDrainRPC"))
	}
	return ShutdownRequest{
		Reason: reason,
		Err:    err,
	}
}
