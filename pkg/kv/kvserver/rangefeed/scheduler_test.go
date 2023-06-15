// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

const (
	work int = 1 << 1
)

type regEvt struct {
	ev int
	id int64
}

func TestScheduler(t *testing.T) {
	s := NewScheduler[int, int64]()

	fmt.Println("enqueue")
	s.Enqueue(1, work)

	fmt.Println("launch processor")
	processed := make(chan regEvt, 1000)
	go s.ProcessEvents(func(event int, id int64) error {
		processed <- regEvt{
			ev: event,
			id: id,
		}
		return nil
	})

	s.Enqueue(2, work)

	<-time.After(time.Second)
	fmt.Println("stop processor")
	s.Stop()

	fmt.Println("dump events")
	close(processed)
	for {
		e, ok := <- processed
		if !ok {
			break
		}
		fmt.Printf("id: %d, ev: %05b\n", e.id, e.ev)
	}
}

func TestSchedCallbacks(t *testing.T) {
	// ctx := context.Background()
	stopper := stop.NewStopper()

	sched := NewCallbackScheduler("nice", NewScheduler(), 3)
	sched.Start(stopper)
	require.NoError(t, sched.Register(1, func(event int) (bool, error) {
		fmt.Printf("1: Event=%08b\n", event)
		return event & Stopped != 0, nil
	}), "failed to register callback")
	sched.Enqueue(1, 1<<3)
	sched.Close()
}
