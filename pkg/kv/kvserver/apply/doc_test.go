// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apply_test

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/apply"
)

func ExampleTask() {
	defer setLogging(true)()
	ctx := context.Background()
	ents := makeEntries(7)

	sm := getTestStateMachine()
	dec := newTestDecoder()
	dec.nonTrivial[5] = true
	dec.nonLocal[2] = true
	dec.nonLocal[6] = true
	dec.shouldReject[3] = true
	dec.shouldReject[6] = true
	fmt.Print(`
Setting up a batch of seven log entries:
 - index 2 and 6 are non-local
 - index 3 and 6 will be rejected
 - index 5 is not trivial
`)

	t := apply.MakeTask(sm, dec)
	defer t.Close()

	fmt.Println("\nDecode (note that index 2 and 6 are not local):")
	if err := t.Decode(ctx, ents); err != nil {
		panic(err)
	}

	fmt.Println("\nAckCommittedEntriesBeforeApplication:")
	if err := t.AckCommittedEntriesBeforeApplication(ctx, 10 /* maxIndex */); err != nil {
		panic(err)
	}
	fmt.Print(`
Above, only index 1 and 4 get acked early. The command at 5 is
non-trivial, so the first batch contains only 1, 2, 3, and 4. An entry
must be in the first batch to qualify for acking early. 2 is not local
(so there's nobody to ack), and 3 is rejected. We can't ack rejected
commands early because the state machine is free to handle them any way
it likes.
`)

	fmt.Println("\nApplyCommittedEntries:")
	if err := t.ApplyCommittedEntries(ctx); err != nil {
		panic(err)
	}
	// Output:
	//
	// Setting up a batch of seven log entries:
	//  - index 2 and 6 are non-local
	//  - index 3 and 6 will be rejected
	//  - index 5 is not trivial
	//
	// Decode (note that index 2 and 6 are not local):
	//  decoding command 1; local=true
	//  decoding command 2; local=false
	//  decoding command 3; local=true
	//  decoding command 4; local=true
	//  decoding command 5; local=true
	//  decoding command 6; local=false
	//  decoding command 7; local=true
	//
	// AckCommittedEntriesBeforeApplication:
	//  acknowledging command 1 before application
	//  acknowledging command 4 before application
	//
	// Above, only index 1 and 4 get acked early. The command at 5 is
	// non-trivial, so the first batch contains only 1, 2, 3, and 4. An entry
	// must be in the first batch to qualify for acking early. 2 is not local
	// (so there's nobody to ack), and 3 is rejected. We can't ack rejected
	// commands early because the state machine is free to handle them any way
	// it likes.
	//
	// ApplyCommittedEntries:
	//  applying batch with commands=[1 2 3 4]
	//  applying side-effects of command 1
	//  applying side-effects of command 2
	//  applying side-effects of command 3
	//  applying side-effects of command 4
	//  finishing command 1; rejected=false
	//  acknowledging and finishing command 2; rejected=false
	//  acknowledging and finishing command 3; rejected=true
	//  finishing command 4; rejected=false
	//  applying batch with commands=[5]
	//  applying side-effects of command 5
	//  acknowledging and finishing command 5; rejected=false
	//  applying batch with commands=[6 7]
	//  applying side-effects of command 6
	//  applying side-effects of command 7
	//  acknowledging and finishing command 6; rejected=true
	//  acknowledging and finishing command 7; rejected=false
}
