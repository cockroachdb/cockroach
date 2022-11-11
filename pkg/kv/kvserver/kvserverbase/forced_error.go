// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvserverbase

// ProposalRejectionType indicates how to handle a proposal that was
// rejected below raft (e.g. by the lease or lease applied index checks).
type ProposalRejectionType int

const (
	// ProposalRejectionPermanent indicates that the rejection is permanent.
	ProposalRejectionPermanent ProposalRejectionType = iota
	// ProposalRejectionIllegalLeaseIndex indicates the proposal failed to apply as the
	// assigned lease index had been consumed, and it is known that this proposal
	// had not applied previously. The command can be retried at a higher lease
	// index.
	ProposalRejectionIllegalLeaseIndex
)
