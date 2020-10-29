// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverpb

//go:generate go run -tags gen-everynode gen_everynode.go

// EveryNodeOp is an interface satisfied by all allowable EveryNode requests.
type EveryNodeOp interface {
	Op() string
}

// EveryNodeOpResp is an interface satisfied by all allowable EveryNode responses.
type EveryNodeOpResp interface {
	everyNodeOpResp()
}

// Op implements the EveryNodeOp interface.
func (a *AckClusterVersionRequest) Op() string { return "ack-cluster-version" }

// everyNodeOpResp implements the EveryNodeOpResp interface.
func (a *AckClusterVersionResponse) everyNodeOpResp() {}

// Op implements the EveryNodeOp interface.
func (a *ValidateTargetClusterVersionRequest) Op() string { return "validate-target-cluster-version" }

// everyNodeOpResp implements the EveryNodeOpResp interface.
func (a *ValidateTargetClusterVersionResponse) everyNodeOpResp() {}
