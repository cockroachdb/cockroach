// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"context"
	"log"
	"sync"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// The following keys are used to tag requests with a specific topic/subscription ID.
var (
	keyTopic        = tag.MustNewKey("topic")
	keySubscription = tag.MustNewKey("subscription")
)

// In the following, errors are used if status is not "OK".
var (
	keyStatus = tag.MustNewKey("status")
	keyError  = tag.MustNewKey("error")
)

const statsPrefix = "cloud.google.com/go/pubsub/"

// The following are measures recorded in publish/subscribe flows.
var (
	// PublishedMessages is a measure of the number of messages published, which may include errors.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PublishedMessages = stats.Int64(statsPrefix+"published_messages", "Number of PubSub message published", stats.UnitDimensionless)

	// PublishLatency is a measure of the number of milliseconds it took to publish a bundle,
	// which may consist of one or more messages.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PublishLatency = stats.Float64(statsPrefix+"publish_roundtrip_latency", "The latency in milliseconds per publish batch", stats.UnitMilliseconds)

	// PullCount is a measure of the number of messages pulled.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PullCount = stats.Int64(statsPrefix+"pull_count", "Number of PubSub messages pulled", stats.UnitDimensionless)

	// AckCount is a measure of the number of messages acked.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	AckCount = stats.Int64(statsPrefix+"ack_count", "Number of PubSub messages acked", stats.UnitDimensionless)

	// NackCount is a measure of the number of messages nacked.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	NackCount = stats.Int64(statsPrefix+"nack_count", "Number of PubSub messages nacked", stats.UnitDimensionless)

	// ModAckCount is a measure of the number of messages whose ack-deadline was modified.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	ModAckCount = stats.Int64(statsPrefix+"mod_ack_count", "Number of ack-deadlines modified", stats.UnitDimensionless)

	// ModAckTimeoutCount is a measure of the number ModifyAckDeadline RPCs that timed out.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	ModAckTimeoutCount = stats.Int64(statsPrefix+"mod_ack_timeout_count", "Number of ModifyAckDeadline RPCs that timed out", stats.UnitDimensionless)

	// StreamOpenCount is a measure of the number of times a streaming-pull stream was opened.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamOpenCount = stats.Int64(statsPrefix+"stream_open_count", "Number of calls opening a new streaming pull", stats.UnitDimensionless)

	// StreamRetryCount is a measure of the number of times a streaming-pull operation was retried.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamRetryCount = stats.Int64(statsPrefix+"stream_retry_count", "Number of retries of a stream send or receive", stats.UnitDimensionless)

	// StreamRequestCount is a measure of the number of requests sent on a streaming-pull stream.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamRequestCount = stats.Int64(statsPrefix+"stream_request_count", "Number gRPC StreamingPull request messages sent", stats.UnitDimensionless)

	// StreamResponseCount is a measure of the number of responses received on a streaming-pull stream.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamResponseCount = stats.Int64(statsPrefix+"stream_response_count", "Number of gRPC StreamingPull response messages received", stats.UnitDimensionless)

	// OutstandingMessages is a measure of the number of outstanding messages held by the client before they are processed.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	OutstandingMessages = stats.Int64(statsPrefix+"outstanding_messages", "Number of outstanding Pub/Sub messages", stats.UnitDimensionless)

	// OutstandingBytes is a measure of the number of bytes all outstanding messages held by the client take up.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	OutstandingBytes = stats.Int64(statsPrefix+"outstanding_bytes", "Number of outstanding bytes", stats.UnitDimensionless)
)

var (
	// PublishedMessagesView is a cumulative sum of PublishedMessages.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PublishedMessagesView *view.View

	// PublishLatencyView is a distribution of PublishLatency.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PublishLatencyView *view.View

	// PullCountView is a cumulative sum of PullCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	PullCountView *view.View

	// AckCountView is a cumulative sum of AckCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	AckCountView *view.View

	// NackCountView is a cumulative sum of NackCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	NackCountView *view.View

	// ModAckCountView is a cumulative sum of ModAckCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	ModAckCountView *view.View

	// ModAckTimeoutCountView is a cumulative sum of ModAckTimeoutCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	ModAckTimeoutCountView *view.View

	// StreamOpenCountView is a cumulative sum of StreamOpenCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamOpenCountView *view.View

	// StreamRetryCountView is a cumulative sum of StreamRetryCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamRetryCountView *view.View

	// StreamRequestCountView is a cumulative sum of StreamRequestCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamRequestCountView *view.View

	// StreamResponseCountView is a cumulative sum of StreamResponseCount.
	// It is EXPERIMENTAL and subject to change or removal without notice.
	StreamResponseCountView *view.View

	// OutstandingMessagesView is the last value of OutstandingMessages
	// It is EXPERIMENTAL and subject to change or removal without notice.
	OutstandingMessagesView *view.View

	// OutstandingBytesView is the last value of OutstandingBytes
	// It is EXPERIMENTAL and subject to change or removal without notice.
	OutstandingBytesView *view.View
)

func init() {
	PublishedMessagesView = createCountView(stats.Measure(PublishedMessages), keyTopic, keyStatus, keyError)
	PublishLatencyView = createDistView(PublishLatency, keyTopic, keyStatus, keyError)
	PullCountView = createCountView(PullCount, keySubscription)
	AckCountView = createCountView(AckCount, keySubscription)
	NackCountView = createCountView(NackCount, keySubscription)
	ModAckCountView = createCountView(ModAckCount, keySubscription)
	ModAckTimeoutCountView = createCountView(ModAckTimeoutCount, keySubscription)
	StreamOpenCountView = createCountView(StreamOpenCount, keySubscription)
	StreamRetryCountView = createCountView(StreamRetryCount, keySubscription)
	StreamRequestCountView = createCountView(StreamRequestCount, keySubscription)
	StreamResponseCountView = createCountView(StreamResponseCount, keySubscription)
	OutstandingMessagesView = createLastValueView(OutstandingMessages, keySubscription)
	OutstandingBytesView = createLastValueView(OutstandingBytes, keySubscription)

	DefaultPublishViews = []*view.View{
		PublishedMessagesView,
		PublishLatencyView,
	}

	DefaultSubscribeViews = []*view.View{
		PullCountView,
		AckCountView,
		NackCountView,
		ModAckCountView,
		ModAckTimeoutCountView,
		StreamOpenCountView,
		StreamRetryCountView,
		StreamRequestCountView,
		StreamResponseCountView,
		OutstandingMessagesView,
		OutstandingBytesView,
	}
}

// These arrays hold the default OpenCensus views that keep track of publish/subscribe operations.
// It is EXPERIMENTAL and subject to change or removal without notice.
var (
	DefaultPublishViews   []*view.View
	DefaultSubscribeViews []*view.View
)

func createCountView(m stats.Measure, keys ...tag.Key) *view.View {
	return &view.View{
		Name:        m.Name(),
		Description: m.Description(),
		TagKeys:     keys,
		Measure:     m,
		Aggregation: view.Sum(),
	}
}

func createDistView(m stats.Measure, keys ...tag.Key) *view.View {
	return &view.View{
		Name:        m.Name(),
		Description: m.Description(),
		TagKeys:     keys,
		Measure:     m,
		Aggregation: view.Distribution(0, 25, 50, 75, 100, 200, 400, 600, 800, 1000, 2000, 4000, 6000),
	}
}

func createLastValueView(m stats.Measure, keys ...tag.Key) *view.View {
	return &view.View{
		Name:        m.Name(),
		Description: m.Description(),
		TagKeys:     keys,
		Measure:     m,
		Aggregation: view.LastValue(),
	}
}

var logOnce sync.Once

// withSubscriptionKey returns a new context modified with the subscriptionKey tag map.
func withSubscriptionKey(ctx context.Context, subName string) context.Context {
	ctx, err := tag.New(ctx, tag.Upsert(keySubscription, subName))
	if err != nil {
		logOnce.Do(func() {
			log.Printf("pubsub: error creating tag map for 'subscribe' key: %v", err)
		})
	}
	return ctx
}

func recordStat(ctx context.Context, m *stats.Int64Measure, n int64) {
	stats.Record(ctx, m.M(n))
}
