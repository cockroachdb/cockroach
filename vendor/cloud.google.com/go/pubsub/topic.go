// Copyright 2016 Google LLC
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
	"errors"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/iam"
	"cloud.google.com/go/internal/optional"
	ipubsub "cloud.google.com/go/internal/pubsub"
	"cloud.google.com/go/pubsub/internal/scheduler"
	gax "github.com/googleapis/gax-go/v2"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"google.golang.org/api/support/bundler"
	pb "google.golang.org/genproto/googleapis/pubsub/v1"
	fmpb "google.golang.org/genproto/protobuf/field_mask"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// MaxPublishRequestCount is the maximum number of messages that can be in
	// a single publish request, as defined by the PubSub service.
	MaxPublishRequestCount = 1000

	// MaxPublishRequestBytes is the maximum size of a single publish request
	// in bytes, as defined by the PubSub service.
	MaxPublishRequestBytes = 1e7
)

// ErrOversizedMessage indicates that a message's size exceeds MaxPublishRequestBytes.
var ErrOversizedMessage = bundler.ErrOversizedItem

// Topic is a reference to a PubSub topic.
//
// The methods of Topic are safe for use by multiple goroutines.
type Topic struct {
	c *Client
	// The fully qualified identifier for the topic, in the format "projects/<projid>/topics/<name>"
	name string

	// Settings for publishing messages. All changes must be made before the
	// first call to Publish. The default is DefaultPublishSettings.
	PublishSettings PublishSettings

	mu        sync.RWMutex
	stopped   bool
	scheduler *scheduler.PublishScheduler

	// EnableMessageOrdering enables delivery of ordered keys.
	EnableMessageOrdering bool
}

// PublishSettings control the bundling of published messages.
type PublishSettings struct {

	// Publish a non-empty batch after this delay has passed.
	DelayThreshold time.Duration

	// Publish a batch when it has this many messages. The maximum is
	// MaxPublishRequestCount.
	CountThreshold int

	// Publish a batch when its size in bytes reaches this value.
	ByteThreshold int

	// The number of goroutines used in each of the data structures that are
	// involved along the the Publish path. Adjusting this value adjusts
	// concurrency along the publish path.
	//
	// Defaults to a multiple of GOMAXPROCS.
	NumGoroutines int

	// The maximum time that the client will attempt to publish a bundle of messages.
	Timeout time.Duration

	// The maximum number of bytes that the Bundler will keep in memory before
	// returning ErrOverflow.
	//
	// Defaults to DefaultPublishSettings.BufferedByteLimit.
	BufferedByteLimit int
}

// DefaultPublishSettings holds the default values for topics' PublishSettings.
var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	// By default, limit the bundler to 10 times the max message size. The number 10 is
	// chosen as a reasonable amount of messages in the worst case whilst still
	// capping the number to a low enough value to not OOM users.
	BufferedByteLimit: 10 * MaxPublishRequestBytes,
}

// CreateTopic creates a new topic.
//
// The specified topic ID must start with a letter, and contain only letters
// ([A-Za-z]), numbers ([0-9]), dashes (-), underscores (_), periods (.),
// tildes (~), plus (+) or percent signs (%). It must be between 3 and 255
// characters in length, and must not start with "goog". For more information,
// see: https://cloud.google.com/pubsub/docs/admin#resource_names
//
// If the topic already exists an error will be returned.
func (c *Client) CreateTopic(ctx context.Context, topicID string) (*Topic, error) {
	t := c.Topic(topicID)
	_, err := c.pubc.CreateTopic(ctx, &pb.Topic{Name: t.name})
	if err != nil {
		return nil, err
	}
	return t, nil
}

// CreateTopicWithConfig creates a topic from TopicConfig.
//
// The specified topic ID must start with a letter, and contain only letters
// ([A-Za-z]), numbers ([0-9]), dashes (-), underscores (_), periods (.),
// tildes (~), plus (+) or percent signs (%). It must be between 3 and 255
// characters in length, and must not start with "goog". For more information,
// see: https://cloud.google.com/pubsub/docs/admin#resource_names.
//
// If the topic already exists, an error will be returned.
func (c *Client) CreateTopicWithConfig(ctx context.Context, topicID string, tc *TopicConfig) (*Topic, error) {
	t := c.Topic(topicID)
	topic := tc.toProto()
	topic.Name = t.name
	_, err := c.pubc.CreateTopic(ctx, topic)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Topic creates a reference to a topic in the client's project.
//
// If a Topic's Publish method is called, it has background goroutines
// associated with it. Clean them up by calling Topic.Stop.
//
// Avoid creating many Topic instances if you use them to publish.
func (c *Client) Topic(id string) *Topic {
	return c.TopicInProject(id, c.projectID)
}

// TopicInProject creates a reference to a topic in the given project.
//
// If a Topic's Publish method is called, it has background goroutines
// associated with it. Clean them up by calling Topic.Stop.
//
// Avoid creating many Topic instances if you use them to publish.
func (c *Client) TopicInProject(id, projectID string) *Topic {
	return newTopic(c, fmt.Sprintf("projects/%s/topics/%s", projectID, id))
}

func newTopic(c *Client, name string) *Topic {
	return &Topic{
		c:               c,
		name:            name,
		PublishSettings: DefaultPublishSettings,
	}
}

// TopicConfig describes the configuration of a topic.
type TopicConfig struct {
	// The set of labels for the topic.
	Labels map[string]string

	// The topic's message storage policy.
	MessageStoragePolicy MessageStoragePolicy

	// The name of the Cloud KMS key to be used to protect access to messages
	// published to this topic, in the format
	// "projects/P/locations/L/keyRings/R/cryptoKeys/K".
	KMSKeyName string

	// Schema defines the schema settings upon topic creation. This cannot
	// be modified after a topic has been created.
	SchemaSettings *SchemaSettings

	// RetentionDuration configures the minimum duration to retain a message
	// after it is published to the topic. If this field is set, messages published
	// to the topic in the last `RetentionDuration` are always available to subscribers.
	// For instance, it allows any attached subscription to [seek to a
	// timestamp](https://cloud.google.com/pubsub/docs/replay-overview#seek_to_a_time)
	// that is up to `RetentionDuration` in the past. If this field is
	// not set, message retention is controlled by settings on individual
	// subscriptions. Cannot be more than 7 days or less than 10 minutes.
	//
	// For more information, see https://cloud.google.com/pubsub/docs/replay-overview#topic_message_retention.
	RetentionDuration optional.Duration
}

func (tc *TopicConfig) toProto() *pb.Topic {
	var retDur *durationpb.Duration
	if tc.RetentionDuration != nil {
		retDur = durationpb.New(optional.ToDuration(tc.RetentionDuration))
	}
	pbt := &pb.Topic{
		Labels:                   tc.Labels,
		MessageStoragePolicy:     messageStoragePolicyToProto(&tc.MessageStoragePolicy),
		KmsKeyName:               tc.KMSKeyName,
		SchemaSettings:           schemaSettingsToProto(tc.SchemaSettings),
		MessageRetentionDuration: retDur,
	}
	return pbt
}

// TopicConfigToUpdate describes how to update a topic.
type TopicConfigToUpdate struct {
	// If non-nil, the current set of labels is completely
	// replaced by the new set.
	Labels map[string]string

	// If non-nil, the existing policy (containing the list of regions)
	// is completely replaced by the new policy.
	//
	// Use the zero value &MessageStoragePolicy{} to reset the topic back to
	// using the organization's Resource Location Restriction policy.
	//
	// If nil, the policy remains unchanged.
	//
	// This field has beta status. It is not subject to the stability guarantee
	// and may change.
	MessageStoragePolicy *MessageStoragePolicy

	// If set to a positive duration between 10 minutes and 7 days, RetentionDuration is changed.
	// If set to a negative value, this clears RetentionDuration from the topic.
	// If nil, the retention duration remains unchanged.
	RetentionDuration optional.Duration
}

func protoToTopicConfig(pbt *pb.Topic) TopicConfig {
	tc := TopicConfig{
		Labels:               pbt.Labels,
		MessageStoragePolicy: protoToMessageStoragePolicy(pbt.MessageStoragePolicy),
		KMSKeyName:           pbt.KmsKeyName,
		SchemaSettings:       protoToSchemaSettings(pbt.SchemaSettings),
	}
	if pbt.GetMessageRetentionDuration() != nil {
		tc.RetentionDuration = pbt.GetMessageRetentionDuration().AsDuration()
	}
	return tc
}

// DetachSubscriptionResult is the response for the DetachSubscription method.
// Reserved for future use.
type DetachSubscriptionResult struct{}

// DetachSubscription detaches a subscription from its topic. All messages
// retained in the subscription are dropped. Subsequent `Pull` and `StreamingPull`
// requests will return FAILED_PRECONDITION. If the subscription is a push
// subscription, pushes to the endpoint will stop.
func (c *Client) DetachSubscription(ctx context.Context, sub string) (*DetachSubscriptionResult, error) {
	_, err := c.pubc.DetachSubscription(ctx, &pb.DetachSubscriptionRequest{
		Subscription: sub,
	})
	if err != nil {
		return nil, err
	}
	return &DetachSubscriptionResult{}, nil
}

// MessageStoragePolicy constrains how messages published to the topic may be stored. It
// is determined when the topic is created based on the policy configured at
// the project level.
type MessageStoragePolicy struct {
	// AllowedPersistenceRegions is the list of GCP regions where messages that are published
	// to the topic may be persisted in storage. Messages published by publishers running in
	// non-allowed GCP regions (or running outside of GCP altogether) will be
	// routed for storage in one of the allowed regions.
	//
	// If empty, it indicates a misconfiguration at the project or organization level, which
	// will result in all Publish operations failing. This field cannot be empty in updates.
	//
	// If nil, then the policy is not defined on a topic level. When used in updates, it resets
	// the regions back to the organization level Resource Location Restriction policy.
	//
	// For more information, see
	// https://cloud.google.com/pubsub/docs/resource-location-restriction#pubsub-storage-locations.
	AllowedPersistenceRegions []string
}

func protoToMessageStoragePolicy(msp *pb.MessageStoragePolicy) MessageStoragePolicy {
	if msp == nil {
		return MessageStoragePolicy{}
	}
	return MessageStoragePolicy{AllowedPersistenceRegions: msp.AllowedPersistenceRegions}
}

func messageStoragePolicyToProto(msp *MessageStoragePolicy) *pb.MessageStoragePolicy {
	if msp == nil || msp.AllowedPersistenceRegions == nil {
		return nil
	}
	return &pb.MessageStoragePolicy{AllowedPersistenceRegions: msp.AllowedPersistenceRegions}
}

// Config returns the TopicConfig for the topic.
func (t *Topic) Config(ctx context.Context) (TopicConfig, error) {
	pbt, err := t.c.pubc.GetTopic(ctx, &pb.GetTopicRequest{Topic: t.name})
	if err != nil {
		return TopicConfig{}, err
	}
	return protoToTopicConfig(pbt), nil
}

// Update changes an existing topic according to the fields set in cfg. It returns
// the new TopicConfig.
func (t *Topic) Update(ctx context.Context, cfg TopicConfigToUpdate) (TopicConfig, error) {
	req := t.updateRequest(cfg)
	if len(req.UpdateMask.Paths) == 0 {
		return TopicConfig{}, errors.New("pubsub: UpdateTopic call with nothing to update")
	}
	rpt, err := t.c.pubc.UpdateTopic(ctx, req)
	if err != nil {
		return TopicConfig{}, err
	}
	return protoToTopicConfig(rpt), nil
}

func (t *Topic) updateRequest(cfg TopicConfigToUpdate) *pb.UpdateTopicRequest {
	pt := &pb.Topic{Name: t.name}
	var paths []string
	if cfg.Labels != nil {
		pt.Labels = cfg.Labels
		paths = append(paths, "labels")
	}
	if cfg.MessageStoragePolicy != nil {
		pt.MessageStoragePolicy = messageStoragePolicyToProto(cfg.MessageStoragePolicy)
		paths = append(paths, "message_storage_policy")
	}
	if cfg.RetentionDuration != nil {
		r := optional.ToDuration(cfg.RetentionDuration)
		pt.MessageRetentionDuration = durationpb.New(r)
		if r < 0 {
			// Clear MessageRetentionDuration if sentinel value is read.
			pt.MessageRetentionDuration = nil
		}
		paths = append(paths, "message_retention_duration")
	}
	return &pb.UpdateTopicRequest{
		Topic:      pt,
		UpdateMask: &fmpb.FieldMask{Paths: paths},
	}
}

// Topics returns an iterator which returns all of the topics for the client's project.
func (c *Client) Topics(ctx context.Context) *TopicIterator {
	it := c.pubc.ListTopics(ctx, &pb.ListTopicsRequest{Project: c.fullyQualifiedProjectName()})
	return &TopicIterator{
		c: c,
		next: func() (string, error) {
			topic, err := it.Next()
			if err != nil {
				return "", err
			}
			return topic.Name, nil
		},
	}
}

// TopicIterator is an iterator that returns a series of topics.
type TopicIterator struct {
	c    *Client
	next func() (string, error)
}

// Next returns the next topic. If there are no more topics, iterator.Done will be returned.
func (tps *TopicIterator) Next() (*Topic, error) {
	topicName, err := tps.next()
	if err != nil {
		return nil, err
	}
	return newTopic(tps.c, topicName), nil
}

// ID returns the unique identifier of the topic within its project.
func (t *Topic) ID() string {
	slash := strings.LastIndex(t.name, "/")
	if slash == -1 {
		// name is not a fully-qualified name.
		panic("bad topic name")
	}
	return t.name[slash+1:]
}

// String returns the printable globally unique name for the topic.
func (t *Topic) String() string {
	return t.name
}

// Delete deletes the topic.
func (t *Topic) Delete(ctx context.Context) error {
	return t.c.pubc.DeleteTopic(ctx, &pb.DeleteTopicRequest{Topic: t.name})
}

// Exists reports whether the topic exists on the server.
func (t *Topic) Exists(ctx context.Context) (bool, error) {
	if t.name == "_deleted-topic_" {
		return false, nil
	}
	_, err := t.c.pubc.GetTopic(ctx, &pb.GetTopicRequest{Topic: t.name})
	if err == nil {
		return true, nil
	}
	if status.Code(err) == codes.NotFound {
		return false, nil
	}
	return false, err
}

// IAM returns the topic's IAM handle.
func (t *Topic) IAM() *iam.Handle {
	return iam.InternalNewHandle(t.c.pubc.Connection(), t.name)
}

// Subscriptions returns an iterator which returns the subscriptions for this topic.
//
// Some of the returned subscriptions may belong to a project other than t.
func (t *Topic) Subscriptions(ctx context.Context) *SubscriptionIterator {
	it := t.c.pubc.ListTopicSubscriptions(ctx, &pb.ListTopicSubscriptionsRequest{
		Topic: t.name,
	})
	return &SubscriptionIterator{
		c:    t.c,
		next: it.Next,
	}
}

var errTopicStopped = errors.New("pubsub: Stop has been called for this topic")

// A PublishResult holds the result from a call to Publish.
//
// Call Get to obtain the result of the Publish call. Example:
//   // Get blocks until Publish completes or ctx is done.
//   id, err := r.Get(ctx)
//   if err != nil {
//       // TODO: Handle error.
//   }
type PublishResult = ipubsub.PublishResult

// Publish publishes msg to the topic asynchronously. Messages are batched and
// sent according to the topic's PublishSettings. Publish never blocks.
//
// Publish returns a non-nil PublishResult which will be ready when the
// message has been sent (or has failed to be sent) to the server.
//
// Publish creates goroutines for batching and sending messages. These goroutines
// need to be stopped by calling t.Stop(). Once stopped, future calls to Publish
// will immediately return a PublishResult with an error.
func (t *Topic) Publish(ctx context.Context, msg *Message) *PublishResult {
	r := ipubsub.NewPublishResult()
	if !t.EnableMessageOrdering && msg.OrderingKey != "" {
		ipubsub.SetPublishResult(r, "", errors.New("Topic.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on Topic.EnableMessageOrdering"))
		return r
	}

	// Use a PublishRequest with only the Messages field to calculate the size
	// of an individual message. This accurately calculates the size of the
	// encoded proto message by accounting for the length of an individual
	// PubSubMessage and Data/Attributes field.
	// TODO(hongalex): if this turns out to take significant time, try to approximate it.
	msgSize := proto.Size(&pb.PublishRequest{
		Messages: []*pb.PubsubMessage{
			{
				Data:        msg.Data,
				Attributes:  msg.Attributes,
				OrderingKey: msg.OrderingKey,
			},
		},
	})
	t.initBundler()
	t.mu.RLock()
	defer t.mu.RUnlock()
	// TODO(aboulhosn) [from bcmills] consider changing the semantics of bundler to perform this logic so we don't have to do it here
	if t.stopped {
		ipubsub.SetPublishResult(r, "", errTopicStopped)
		return r
	}

	// TODO(jba) [from bcmills] consider using a shared channel per bundle
	// (requires Bundler API changes; would reduce allocations)
	err := t.scheduler.Add(msg.OrderingKey, &bundledMessage{msg, r}, msgSize)
	if err != nil {
		t.scheduler.Pause(msg.OrderingKey)
		ipubsub.SetPublishResult(r, "", err)
	}
	return r
}

// Stop sends all remaining published messages and stop goroutines created for handling
// publishing. Returns once all outstanding messages have been sent or have
// failed to be sent.
func (t *Topic) Stop() {
	t.mu.Lock()
	noop := t.stopped || t.scheduler == nil
	t.stopped = true
	t.mu.Unlock()
	if noop {
		return
	}
	t.scheduler.FlushAndStop()
}

// Flush blocks until all remaining messages are sent.
func (t *Topic) Flush() {
	if t.stopped || t.scheduler == nil {
		return
	}
	t.scheduler.Flush()
}

type bundledMessage struct {
	msg *Message
	res *PublishResult
}

func (t *Topic) initBundler() {
	t.mu.RLock()
	noop := t.stopped || t.scheduler != nil
	t.mu.RUnlock()
	if noop {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Must re-check, since we released the lock.
	if t.stopped || t.scheduler != nil {
		return
	}

	timeout := t.PublishSettings.Timeout

	workers := t.PublishSettings.NumGoroutines
	// Unless overridden, allow many goroutines per CPU to call the Publish RPC
	// concurrently. The default value was determined via extensive load
	// testing (see the loadtest subdirectory).
	if t.PublishSettings.NumGoroutines == 0 {
		workers = 25 * runtime.GOMAXPROCS(0)
	}

	t.scheduler = scheduler.NewPublishScheduler(workers, func(bundle interface{}) {
		// TODO(jba): use a context detached from the one passed to NewClient.
		ctx := context.TODO()
		if timeout != 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}
		t.publishMessageBundle(ctx, bundle.([]*bundledMessage))
	})
	t.scheduler.DelayThreshold = t.PublishSettings.DelayThreshold
	t.scheduler.BundleCountThreshold = t.PublishSettings.CountThreshold
	if t.scheduler.BundleCountThreshold > MaxPublishRequestCount {
		t.scheduler.BundleCountThreshold = MaxPublishRequestCount
	}
	t.scheduler.BundleByteThreshold = t.PublishSettings.ByteThreshold

	bufferedByteLimit := DefaultPublishSettings.BufferedByteLimit
	if t.PublishSettings.BufferedByteLimit > 0 {
		bufferedByteLimit = t.PublishSettings.BufferedByteLimit
	}
	t.scheduler.BufferedByteLimit = bufferedByteLimit

	t.scheduler.BundleByteLimit = MaxPublishRequestBytes - calcFieldSizeString(t.name)
}

func (t *Topic) publishMessageBundle(ctx context.Context, bms []*bundledMessage) {
	ctx, err := tag.New(ctx, tag.Insert(keyStatus, "OK"), tag.Upsert(keyTopic, t.name))
	if err != nil {
		log.Printf("pubsub: cannot create context with tag in publishMessageBundle: %v", err)
	}
	pbMsgs := make([]*pb.PubsubMessage, len(bms))
	var orderingKey string
	for i, bm := range bms {
		orderingKey = bm.msg.OrderingKey
		pbMsgs[i] = &pb.PubsubMessage{
			Data:        bm.msg.Data,
			Attributes:  bm.msg.Attributes,
			OrderingKey: bm.msg.OrderingKey,
		}
		bm.msg = nil // release bm.msg for GC
	}
	var res *pb.PublishResponse
	start := time.Now()
	if orderingKey != "" && t.scheduler.IsPaused(orderingKey) {
		err = fmt.Errorf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", orderingKey)
	} else {
		res, err = t.c.pubc.Publish(ctx, &pb.PublishRequest{
			Topic:    t.name,
			Messages: pbMsgs,
		}, gax.WithGRPCOptions(grpc.MaxCallSendMsgSize(maxSendRecvBytes)))
	}
	end := time.Now()
	if err != nil {
		t.scheduler.Pause(orderingKey)
		// Update context with error tag for OpenCensus,
		// using same stats.Record() call as success case.
		ctx, _ = tag.New(ctx, tag.Upsert(keyStatus, "ERROR"),
			tag.Upsert(keyError, err.Error()))
	}
	stats.Record(ctx,
		PublishLatency.M(float64(end.Sub(start)/time.Millisecond)),
		PublishedMessages.M(int64(len(bms))))
	for i, bm := range bms {
		if err != nil {
			ipubsub.SetPublishResult(bm.res, "", err)
		} else {
			ipubsub.SetPublishResult(bm.res, res.MessageIds[i], nil)
		}
	}
}

// ResumePublish resumes accepting messages for the provided ordering key.
// Publishing using an ordering key might be paused if an error is
// encountered while publishing, to prevent messages from being published
// out of order.
func (t *Topic) ResumePublish(orderingKey string) {
	t.mu.RLock()
	noop := t.scheduler == nil
	t.mu.RUnlock()
	if noop {
		return
	}

	t.scheduler.Resume(orderingKey)
}
