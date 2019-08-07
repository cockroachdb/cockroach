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

	"github.com/cockroachdb/cockroach/pkg/storage/apply"
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

	t := apply.MakeTask(sm, dec)
	defer t.Close()

	fmt.Println("Decode:")
	if err := t.Decode(ctx, ents); err != nil {
		panic(err)
	}

	fmt.Println("\nApplyCommittedEntries:")
	if err := t.ApplyCommittedEntries(ctx); err != nil {
		panic(err)
	}
	// Output:
	//
	// Decode:
	//  decoding command 1; local=true
	//  decoding command 2; local=false
	//  decoding command 3; local=true
	//  decoding command 4; local=true
	//  decoding command 5; local=true
	//  decoding command 6; local=false
	//  decoding command 7; local=true
	//
	// ApplyCommittedEntries:
	//  committing batch with commands=[1 2 3 4]
	//  applying side-effects of command 1
	//  applying side-effects of command 2
	//  applying side-effects of command 3
	//  applying side-effects of command 4
	//  finishing and acknowledging command 1; rejected=false
	//  finishing and acknowledging command 2; rejected=false
	//  finishing and acknowledging command 3; rejected=true
	//  finishing and acknowledging command 4; rejected=false
	//  committing batch with commands=[5]
	//  applying side-effects of command 5
	//  finishing and acknowledging command 5; rejected=false
	//  committing batch with commands=[6 7]
	//  applying side-effects of command 6
	//  applying side-effects of command 7
	//  finishing and acknowledging command 6; rejected=true
	//  finishing and acknowledging command 7; rejected=false
}
