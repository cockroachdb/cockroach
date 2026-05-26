// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"cmp"
	"context"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type kvEvent struct {
	key  string
	time int
}

func (a kvEvent) Compare(b kvEvent) int {
	if c := cmp.Compare(a.time, b.time); c != 0 {
		return c
	}
	return cmp.Compare(a.key, b.key)
}

type checkpoint struct {
	start string
	end   string
	time  int
}

// closeEvent is a sentinel that causes testSubscription to close its output
// channel, signaling stream completion.
type closeEvent struct{}

type testSubscription struct {
	events []any
	output chan crosscluster.Event
}

func (s *testSubscription) Subscribe(ctx context.Context) error {
	for _, ev := range s.events {
		switch e := ev.(type) {
		case kvEvent:
			s.output <- crosscluster.MakeKVEvent([]streampb.StreamEvent_KV{
				{
					KeyValue: roachpb.KeyValue{
						Key: roachpb.Key(e.key),
						Value: roachpb.Value{
							Timestamp: hlc.Timestamp{WallTime: int64(e.time)},
						},
					},
				},
			})
		case []kvEvent:
			kvs := make([]streampb.StreamEvent_KV, len(e))
			for i, kv := range e {
				kvs[i] = streampb.StreamEvent_KV{
					KeyValue: roachpb.KeyValue{
						Key: roachpb.Key(kv.key),
						Value: roachpb.Value{
							Timestamp: hlc.Timestamp{WallTime: int64(kv.time)},
						},
					},
				}
			}
			s.output <- crosscluster.MakeKVEvent(kvs)
		case checkpoint:
			s.output <- crosscluster.MakeCheckpointEvent(&streampb.StreamEvent_StreamCheckpoint{
				ResolvedSpans: []jobspb.ResolvedSpan{
					{
						Span: roachpb.Span{
							Key:    roachpb.Key(e.start),
							EndKey: roachpb.Key(e.end),
						},
						Timestamp: hlc.Timestamp{WallTime: int64(e.time)},
					},
				},
			})
		case closeEvent:
			close(s.output)
			return nil
		}
	}
	return nil
}

func (s *testSubscription) Events() <-chan crosscluster.Event {
	return s.output
}

func (s *testSubscription) Err() error {
	return nil
}

func makeTestSubscription(events []any) *testSubscription {
	ch := make(chan crosscluster.Event, len(events))
	return &testSubscription{events: events, output: ch}
}
