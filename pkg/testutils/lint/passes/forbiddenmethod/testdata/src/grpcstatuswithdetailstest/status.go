// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcstatuswithdetailstest

import "google.golang.org/grpc/status"

func F() {
	s := status.New(1, "message")
	_, _ = s.WithDetails() // want `Illegal call to Status.WithDetails\(\)`

	//nolint:grpcstatuswithdetails
	_, _ = s.WithDetails()
}
