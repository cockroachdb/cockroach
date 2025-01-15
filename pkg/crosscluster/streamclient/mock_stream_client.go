// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
)

// MockStreamClient will return the slice of events associated to the stream
// partition being consumed. Stream partitions are identified by unique
// partition connection uris.
type MockStreamClient struct {
	PartitionEvents          map[string][]crosscluster.Event
	DoneCh                   chan struct{}
	HeartbeatErr             error
	HeartbeatStatus          streampb.StreamReplicationStatus
	OnHeartbeat              func() (streampb.StreamReplicationStatus, error)
	OnPlanLogicalReplication func(streampb.LogicalReplicationPlanRequest) (LogicalReplicationPlan, error)
}

var _ Client = &MockStreamClient{}

// CreateForTenant implements the Client interface.
func (m *MockStreamClient) CreateForTenant(
	_ context.Context, _ roachpb.TenantName, _ streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	panic("unimplemented")
}

// Dial implements the Client interface.
func (m *MockStreamClient) Dial(_ context.Context) error {
	panic("unimplemented")
}

// Heartbeat implements the Client interface.
func (m *MockStreamClient) Heartbeat(
	_ context.Context, _ streampb.StreamID, _ hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	if m.OnHeartbeat != nil {
		return m.OnHeartbeat()
	}
	return m.HeartbeatStatus, m.HeartbeatErr
}

// PlanPhysicalReplication implements the Client interface.
func (m *MockStreamClient) PlanPhysicalReplication(
	_ context.Context, _ streampb.StreamID,
) (Topology, error) {
	panic("unimplemented mock method")
}

type mockSubscription struct {
	eventsCh chan crosscluster.Event
}

// Subscribe implements the Subscription interface.
func (m *mockSubscription) Subscribe(_ context.Context) error {
	return nil
}

// Events implements the Subscription interface.
func (m *mockSubscription) Events() <-chan crosscluster.Event {
	return m.eventsCh
}

// Err implements the Subscription interface.
func (m *mockSubscription) Err() error {
	return nil
}

// Subscribe implements the Client interface.
func (m *MockStreamClient) Subscribe(
	ctx context.Context,
	_ streampb.StreamID,
	_, _ int32,
	token SubscriptionToken,
	initialScanTime hlc.Timestamp,
	_ span.Frontier,
	_ ...SubscribeOption,
) (Subscription, error) {
	var events []crosscluster.Event
	var ok bool
	if events, ok = m.PartitionEvents[string(token)]; !ok {
		return nil, errors.Newf("no events found for partition %s", string(token))
	}
	log.Infof(ctx, "%q beginning subscription from time %v ", string(token), initialScanTime)

	log.Infof(ctx, "%q emitting %d events", string(token), len(events))
	eventCh := make(chan crosscluster.Event, len(events))
	for _, event := range events {
		log.Infof(ctx, "%q emitting event %v", string(token), event)
		eventCh <- event
	}
	log.Infof(ctx, "%q done emitting %d events", string(token), len(events))
	go func() {
		if m.DoneCh != nil {
			log.Infof(ctx, "%q waiting for doneCh", string(token))
			<-m.DoneCh
			log.Infof(ctx, "%q received event on doneCh", string(token))
		}
		close(eventCh)
	}()
	return &mockSubscription{eventsCh: eventCh}, nil
}

// Close implements the Client interface.
func (m *MockStreamClient) Close(_ context.Context) error {
	return nil
}

// Complete implements the streamclient.Client interface.
func (m *MockStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

// PriorReplicationDetails implements the streamclient.Client interface.
func (m *MockStreamClient) PriorReplicationDetails(
	_ context.Context, _ roachpb.TenantName,
) (string, string, hlc.Timestamp, error) {
	return "", "", hlc.Timestamp{}, nil
}

func (p *MockStreamClient) PlanLogicalReplication(
	_ context.Context, req streampb.LogicalReplicationPlanRequest,
) (LogicalReplicationPlan, error) {
	if p.OnPlanLogicalReplication != nil {
		return p.OnPlanLogicalReplication(req)
	}
	return LogicalReplicationPlan{}, errors.AssertionFailedf("no onPlanLogicalReplication callback provided")
}

func (p *MockStreamClient) CreateForTables(
	ctx context.Context, req *streampb.ReplicationProducerRequest,
) (*streampb.ReplicationProducerSpec, error) {
	return nil, errors.AssertionFailedf("unimplemented")
}

func (p *MockStreamClient) ExecStatement(
	ctx context.Context, cmd string, opname string, args ...interface{},
) error {
	return errors.AssertionFailedf("unimplemented")
}

// ErrorStreamClient always returns an error when consuming a partition.
type ErrorStreamClient struct{ MockStreamClient }

var _ Client = &ErrorStreamClient{}

// ConsumePartition implements the streamclient.Client interface.
func (m *ErrorStreamClient) Subscribe(
	_ context.Context,
	_ streampb.StreamID,
	_, _ int32,
	_ SubscriptionToken,
	_ hlc.Timestamp,
	_ span.Frontier,
	_ ...SubscribeOption,
) (Subscription, error) {
	return nil, errors.New("this client always returns an error")
}

// Complete implements the streamclient.Client interface.
func (m *ErrorStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}
