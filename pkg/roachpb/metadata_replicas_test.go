// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package roachpb

import "testing"

func TestVotersLearners(t *testing.T) {
	tests := [][]ReplicaDescriptor{
		{},
		{{Type: ReplicaType_VOTER}},
		{{Type: ReplicaType_LEARNER}},
		{{Type: ReplicaType_VOTER}, {Type: ReplicaType_LEARNER}, {Type: ReplicaType_VOTER}},
		{{Type: ReplicaType_LEARNER}, {Type: ReplicaType_VOTER}, {Type: ReplicaType_LEARNER}},
	}
	for i, test := range tests {
		r := MakeReplicaDescriptors(test)
		for _, voter := range r.Voters() {
			if voter.Type != ReplicaType_VOTER {
				t.Errorf(`%d: contained a %s in Voters`, i, voter.Type)
			}
		}
		for _, learner := range r.Learners() {
			if learner.Type != ReplicaType_LEARNER {
				t.Errorf(`%d: contained a %s in Learners`, i, learner.Type)
			}
		}
	}
}
