// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package transport_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/transport"
	transporttestutils "github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/transport/testutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/kr/pretty"
)

// NewTestContainer sets up an environment suitable for black box testing the
// transport subsystem. The returned test container contains most notably a
// Clients and Server set up to communicate to each other via a Dialer (which
// keeps a transcript that can be verified).
func NewTestContainer() *TestContainer {
	stopper := stop.NewStopper()

	st := cluster.MakeTestingClusterSettings()
	p := &TestProducer{}
	sink := newTestNotifyee(stopper)
	refreshed := &RefreshTracker{}
	s := transport.NewServer(stopper, p, refreshed.Add)
	dialer := transporttestutils.NewChanDialer(stopper, s)
	c := transport.NewClients(transport.Config{
		NodeID:   roachpb.NodeID(12345),
		Settings: st,
		Stopper:  stopper,
		Dialer:   dialer,
		Sink:     sink,
	})
	return &TestContainer{
		Settings:  st,
		Stopper:   stopper,
		Producer:  p,
		Notifyee:  sink,
		Refreshed: refreshed,
		Server:    s,
		Dialer:    dialer,
		Clients:   c,
	}
}

func assertNumSubscribers(t *testing.T, p *TestProducer, exp int) {
	testutils.SucceedsSoon(t, func() error {
		n := p.numSubscriptions()
		if n > exp {
			t.Fatalf("expected a single subscription, got %d", n)
		}
		if n < exp {
			return errors.New("waiting for subscription")
		}
		return nil
	})
}

func TestTransportConnectOnRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, closedts.IssueTrackingRemovalOfOldClosedTimestampsCode)

	container := NewTestContainer()
	defer container.Stopper.Stop(context.Background())

	const (
		nodeID  = 1
		rangeID = 13
	)

	// Requesting an update for a Range implies a connection attempt.
	container.Clients.Request(nodeID, rangeID)

	// Find the connection (via its subscription to receive new Entries).
	assertNumSubscribers(t, container.Producer, 1)

	// Verify that the client soon asks the server for an update for this range.
	testutils.SucceedsSoon(t, func() error {
		act := container.Refreshed.Get()
		exp := []roachpb.RangeID{rangeID}

		if diff := pretty.Diff(act, exp); len(diff) != 0 {
			// We have to kick the tires a little bit. The client can only send
			// the request as the reaction to an Entry.
			container.Producer.sendAll(ctpb.Entry{})
			return errors.Errorf("diff(act, exp): %s", strings.Join(diff, "\n"))
		}
		return nil
	})
}

func TestTransportClientReceivesEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, closedts.IssueTrackingRemovalOfOldClosedTimestampsCode)

	container := NewTestContainer()
	defer container.Stopper.Stop(context.Background())

	const nodeID = 7

	// Manual reconnections don't spawn new clients.
	container.Clients.EnsureClient(nodeID)
	container.Clients.EnsureClient(nodeID)
	container.Clients.EnsureClient(nodeID)
	assertNumSubscribers(t, container.Producer, 1)

	// But connecting to other nodes does (only once).
	for i := 0; i < 7; i++ {
		container.Clients.EnsureClient(nodeID + 1)
		container.Clients.EnsureClient(nodeID + 2)
		container.Clients.Request(nodeID+3, roachpb.RangeID(7))
	}
	assertNumSubscribers(t, container.Producer, 4)

	// Our initial client doesn't do anything except say "hello" via
	// a Reaction.
	testutils.SucceedsSoon(t, func() error {
		expectedTranscript := []interface{}{
			&ctpb.Reaction{},
		}
		return checkTranscript(t, container.Dialer.Transcript(nodeID), expectedTranscript)
	})

	// Now the producer (to which the server should maintain a subscription for this client, and
	// notifications from which it should relay) emits an Entry.
	e1 := ctpb.Entry{ClosedTimestamp: hlc.Timestamp{WallTime: 1e9}, Epoch: 12, MLAI: map[roachpb.RangeID]ctpb.LAI{12: 7}}
	container.Producer.sendAll(e1)

	// The client should see this entry soon thereafter. it responds with an empty
	// Reaction (since we haven't Request()ed anything).
	testutils.SucceedsSoon(t, func() error {
		expectedTranscript := []interface{}{
			&ctpb.Reaction{},
			&e1,
			&ctpb.Reaction{},
		}
		return checkTranscript(t, container.Dialer.Transcript(nodeID), expectedTranscript)
	})

	// And again, but only after Request() is called (which should be reflected in the transcript).
	const rangeID = 7
	container.Clients.Request(nodeID, rangeID)
	e2 := ctpb.Entry{ClosedTimestamp: hlc.Timestamp{WallTime: 2e9}, Epoch: 13, MLAI: map[roachpb.RangeID]ctpb.LAI{13: 8}}
	container.Producer.sendAll(e2)
	testutils.SucceedsSoon(t, func() error {
		expectedTranscript := []interface{}{
			&ctpb.Reaction{},
			&e1,
			&ctpb.Reaction{},
			&e2,
			&ctpb.Reaction{Requested: []roachpb.RangeID{rangeID}},
		}
		return checkTranscript(t, container.Dialer.Transcript(nodeID), expectedTranscript)
	})

}

func checkTranscript(t *testing.T, actI, expI []interface{}) error {
	t.Helper()
	var act, exp []string
	for _, i := range actI {
		act = append(act, strings.TrimSpace(fmt.Sprintf("%v", i)))
	}
	for _, i := range expI {
		exp = append(exp, strings.TrimSpace(fmt.Sprintf("%v", i)))
	}

	diffErr := errors.Errorf("actual:\n%s\nexpected:\n%s", strings.Join(act, "\n"), strings.Join(exp, "\n"))
	if len(act) > len(exp) {
		t.Fatal(errors.Wrap(diffErr, "actual transcript longer than expected"))
	}
	if len(act) < len(exp) {
		return errors.Wrap(diffErr, "waiting for more")
	}
	if diff := pretty.Diff(actI, expI); len(diff) != 0 {
		t.Fatal(errors.Wrapf(diffErr, "diff:\n%v\n", strings.Join(diff, "\n")))
	}
	return nil
}
