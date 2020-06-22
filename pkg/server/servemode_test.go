// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

func TestWaitingForInitError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := &grpcServer{}
	if err := s.waitingForInitError("foo"); !IsWaitingForInit(err) {
		t.Errorf("WaitingForInitError() not recognized by IsWaitingForInit(): %v", err)
	}
	if err := grpcstatus.Errorf(codes.Unavailable, "foo"); IsWaitingForInit(err) {
		t.Errorf("unavailable error undesirably recognized by IsWaitingForInit(): %v", err)
	}
	if err := fmt.Errorf("node waiting for init"); IsWaitingForInit(err) {
		t.Errorf("non-grpc error undesirably recognized by IsWaitingForInit(): %v", err)
	}
}
