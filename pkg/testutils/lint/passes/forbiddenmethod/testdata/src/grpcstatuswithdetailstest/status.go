// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcstatuswithdetailstest

import "google.golang.org/grpc/status"

func F() {
	s := status.New(1, "message")
	_, _ = s.WithDetails() // want `Illegal call to Status.WithDetails\(\)`

	//nolint:grpcstatuswithdetails
	_, _ = s.WithDetails()
}
