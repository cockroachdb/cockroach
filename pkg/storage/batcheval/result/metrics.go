// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package result

// Metrics tracks various counters related to command applications and
// their outcomes.
type Metrics struct {
	LeaseRequestSuccess  int // lease request evaluated successfully
	LeaseRequestError    int // lease request error at evaluation time
	LeaseTransferSuccess int // lease transfer evaluated successfully
	LeaseTransferError   int // lease transfer error at evaluation time
	ResolveCommit        int // intent commit evaluated successfully
	ResolveAbort         int // non-poisoning intent abort evaluated successfully
	ResolvePoison        int // poisoning intent abort evaluated successfully
}

// Add absorbs the supplied Metrics into the receiver.
func (mt *Metrics) Add(o Metrics) {
	mt.LeaseRequestSuccess += o.LeaseRequestSuccess
	mt.LeaseRequestError += o.LeaseRequestError
	mt.LeaseTransferSuccess += o.LeaseTransferSuccess
	mt.LeaseTransferError += o.LeaseTransferError
	mt.ResolveCommit += o.ResolveCommit
	mt.ResolveAbort += o.ResolveAbort
	mt.ResolvePoison += o.ResolvePoison
}
