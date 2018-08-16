// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
