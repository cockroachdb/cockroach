package notify

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotification"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

// channelQueueSize is a cluster setting that sets the per-channel buffer size
// before notifications are dropped.
var channelQueueSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.notifications.max_pending",
	"the number of outstanding notifications permitted per channel per session before new notifications are dropped",
	1024,
)

// notificationsEnabled is the cluster setting that allows users
// to enable notifications.
var notificationsEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.notifications.enabled",
	"enable or disable the PG NOTIFY/LISTEN feature",
	true,
	settings.WithPublic)

type NotificationSender interface {
	SendNotification(payload pgnotification.Notification) error
}

type ListenerID clusterunique.ID

type channelMux struct {
	mu       syncutil.Mutex
	ch       chan pgnotification.Notification
	senders  map[ListenerID]NotificationSender
	batchBuf []pgnotification.Notification
}

const batchSize = 256

func newChannelMux(queueSize int) *channelMux {
	return &channelMux{
		ch:       make(chan pgnotification.Notification, queueSize),
		senders:  make(map[ListenerID]NotificationSender, 1),
		batchBuf: make([]pgnotification.Notification, 0, batchSize),
	}
}

func (cm *channelMux) AddSender(id ListenerID, s NotificationSender) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// I think it's ok to just overwrite this here.
	cm.senders[id] = s
}

func (cm *channelMux) RemoveSender(id ListenerID) (deleteMe bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.senders, id)

	return len(cm.senders) == 0
}

// NOTE: I think it's possible for a listener to receive a notification from
// before it subscribed, if the rangefeed is behind and maybe for other reasons.
// This MIGHT be ok. I don't think the spec rules it out.

// RunBatch reads a chunk of messages from the channel and sends them to the senders.
//
// TODO: room for improvement:
// - guard against slow clients (without using too many goroutines)
// - handle errors (poison connection? remove listener?)
func (cm *channelMux) RunBatch(ctx context.Context) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Fill up buf with messages to send.
	cm.batchBuf = cm.batchBuf[:0]
fill:
	for len(cm.batchBuf) < batchSize {
		select {
		case <-ctx.Done():
			return
		case n := <-cm.ch:
			cm.batchBuf = append(cm.batchBuf, n)
		default:
			break fill
		}
	}

	if log.V(2) && len(cm.batchBuf) > 0 {
		log.Infof(ctx, "running batch: %d messages", len(cm.batchBuf))
	}
	for _, n := range cm.batchBuf {
		for _, s := range cm.senders {
			if err := s.SendNotification(n); err != nil {
				log.Warningf(ctx, "error sending notification %+v: %v", n, err)
			}
		}
	}
}

// queryRowExer is a subset of isql.InternalExecutor to avoid dependency cycles.
type queryRowExer interface {
	QueryRowEx(
		ctx context.Context,
		opName redact.RedactableString,
		txn *kv.Txn,
		session sessiondata.InternalExecutorOverride,
		stmt string,
		qargs ...interface{},
	) (tree.Datums, error)
}

type ListenerRegistry struct {
	settings *cluster.Settings
	stopper  *stop.Stopper
	sender   *kvcoord.DistSender
	clock    *hlc.Clock
	ie       queryRowExer
	codec    keys.SQLCodec

	Metrics    *Metrics
	timeSource timeutil.TimeSource

	listenersMu struct {
		syncutil.Mutex
		channels map[string]*channelMux
		// idChannels is a map from listener ID to channel name. This is used to
		// implement UNLISTEN more efficiently.
		idChannels map[ListenerID][]string
	}
}

func NewRegistry(
	settings *cluster.Settings,
	stopper *stop.Stopper,
	sender *kvcoord.DistSender,
	clock *hlc.Clock,
	ie queryRowExer,
	codec keys.SQLCodec,
) *ListenerRegistry {
	r := &ListenerRegistry{
		settings:   settings,
		stopper:    stopper,
		sender:     sender,
		clock:      clock,
		ie:         ie,
		codec:      codec,
		timeSource: timeutil.DefaultTimeSource{},
		Metrics:    NewMetrics(10 * time.Second),
	}
	r.listenersMu.channels = make(map[string]*channelMux)
	r.listenersMu.idChannels = make(map[ListenerID][]string)
	return r
}

func (r *ListenerRegistry) AddListener(ctx context.Context, id ListenerID, channel string, sender NotificationSender) error {
	if !notificationsEnabled.Get(&r.settings.SV) {
		return errors.New("notifications are disabled. Set 'sql.notifications.enabled' to true to enable them")
	}

	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	var cm *channelMux
	var ok bool

	if cm, ok = r.listenersMu.channels[channel]; !ok {
		cm = newChannelMux(int(channelQueueSize.Get(&r.settings.SV)))
		r.listenersMu.channels[channel] = cm
	}

	if log.V(2) {
		log.Infof(ctx, "adding listener %d to %s", id, channel)
	}

	cm.AddSender(id, sender)
	r.listenersMu.idChannels[id] = append(r.listenersMu.idChannels[id], channel)

	r.Metrics.NumChannels.Update(int64(len(r.listenersMu.channels)))

	return nil
}

func (r *ListenerRegistry) RemoveListener(ctx context.Context, id ListenerID, channel string) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if log.V(2) {
		log.Infof(ctx, "removing listener %d from %s", id, channel)
	}

	if del := r.listenersMu.channels[channel].RemoveSender(id); del {
		delete(r.listenersMu.channels, channel)
	}
	delete(r.listenersMu.idChannels, id)
}

func (r *ListenerRegistry) RemoveAllListeners(ctx context.Context, id ListenerID) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if log.V(2) {
		log.Infof(ctx, "removing all listeners for %d", id)
	}

	for _, channel := range r.listenersMu.idChannels[id] {
		if del := r.listenersMu.channels[channel].RemoveSender(id); del {
			delete(r.listenersMu.channels, channel)
		}
	}
	delete(r.listenersMu.idChannels, id)
}

// notify feeds a notification into the appropriate channelMux. It will drop the
// notification if the channel is full.
func (r *ListenerRegistry) notify(ctx context.Context, channel string, payload string, pid int32) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if cm, ok := r.listenersMu.channels[channel]; ok {
		notif := pgnotification.Notification{Channel: channel, Payload: payload, PID: pid}
		select {
		case cm.ch <- notif:
			if log.V(2) {
				log.Infof(ctx, "sent notification %+v", notif)
			}
		default:
			log.Warningf(ctx, "dropping notification %+v on channel %s", notif, channel)
			r.Metrics.Dropped.Inc(1)
		}
	}
}

func (r *ListenerRegistry) Start(ctx context.Context) {
	_ = r.stopper.RunAsyncTask(ctx, "listener_registry_run_batches", r.runBatches)
	_ = r.stopper.RunTask(ctx, "listener_registry_run_rangefeed", r.runRangefeed)
}

const tickIvl = 100 * time.Millisecond

// runBatches drives notification forwarding by calling channelMux.RunBatch on all channelMuxes.
func (r *ListenerRegistry) runBatches(ctx context.Context) {
	ctx, cancel := r.stopper.WithCancelOnQuiesce(ctx)
	defer cancel()

	ticker := r.timeSource.NewTicker(tickIvl)
	defer ticker.Stop()

	var cms []*channelMux

	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case <-ticker.Ch():
		}

		// Grab a snapshot of the channels so we don't hold the lock while running batches.
		func() {
			r.listenersMu.Lock()
			defer r.listenersMu.Unlock()

			cms = cms[:0]
			for _, cm := range r.listenersMu.channels {
				cms = append(cms, cm)
			}
		}()

		// TODO: do this with some constant parallelism to improve performance.
		for _, cm := range cms {
			cm.RunBatch(ctx)
		}
	}
}

// runRangefeed starts a rangefeed on the system.notifications table and sends the results to the appropriate channelMuxes.
func (r *ListenerRegistry) runRangefeed(ctx context.Context) {
	ctx, _ = r.stopper.WithCancelOnQuiesce(ctx) // this ctx is passed to spawned tasks so dont cancel it

	// if the table doesnt exist, wait until it does
	if !r.settings.Version.IsActive(ctx, clusterversion.V25_1_ListenNotifyQueue) {
		ticker := r.timeSource.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Ch():
			}
			if r.settings.Version.IsActive(ctx, clusterversion.V25_1_ListenNotifyQueue) {
				break
			}
		}
	}

	// Get the notifications table's span.
	res, err := startup.RunIdempotentWithRetryEx(ctx, r.stopper.ShouldQuiesce(), "fetch-pg-notification-table-id", func(ctx context.Context) (tree.Datums, error) {
		return r.ie.QueryRowEx(ctx, "fetch-pg-notification-table-id", nil, sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`SELECT 'system.%s'::regclass::oid`, catconstants.ListenNotifyQueueTableName))
	})
	if err != nil {
		log.Errorf(ctx, "failed to start notification system: %v", err)
		return
	}
	oid := tree.MustBeDOid(res[0])
	prefix := r.codec.TablePrefix(uint32(oid.Oid))
	ch := make(chan kvcoord.RangeFeedMessage, 1024)

	span := roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}

	startTs := atomic.Value{}
	startTs.Store(r.clock.Now())

	// Run the rangefeed in a goroutine, retrying forever.
	err = r.stopper.RunAsyncTask(ctx, "listener_registry_rangefeed", func(ctx context.Context) {
		for ctx.Err() == nil {
			log.Infof(ctx, "starting rangefeed")
			spansTimes := []kvcoord.SpanTimePair{
				{Span: span, StartAfter: startTs.Load().(hlc.Timestamp)},
			}
			if err := r.sender.RangeFeed(ctx, spansTimes, ch); err != nil {
				log.Warningf(ctx, "notifications rangefeed failed: %v", err)
			}
			r.Metrics.RangefeedRestarts.Inc(1)
		}
	})
	if err != nil {
		log.Warningf(ctx, "failed to run rangefeed: %v", err)
		return
	}

	// Feed the rangefeed messages into the registry forever.
	_ = r.stopper.RunAsyncTask(ctx, "listener_registry_rangefeed_consumer", func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				if log.V(2) {
					log.Infof(ctx, "received rangefeed msg: %+v", msg)
				}

				if msg.Error != nil {
					// TODO: when can this happen and what does it mean?
					continue
				}
				val := msg.Val
				if val == nil {
					// TODO: when can this happen and what does it mean?
					continue
				}

				channel, payload, pid, err := decodeRangefeedVal(val)
				if err != nil {
					log.Warningf(ctx, "failed to decode rangefeed msg (%+#v): %v", val, err)
					continue
				}

				r.notify(ctx, channel, payload, pid)

				// Update the start timestamp to the latest timestamp we've sent down the pipe.
				startTs.Store(msg.Val.Timestamp())
			}
		}
	})
}

func decodeRangefeedVal(val *kvpb.RangeFeedValue) (channel, payload string, pid int32, err error) {
	key, _, _ := keys.DecodeTenantPrefix(val.Key)
	key, _, _, err = keys.DecodeTableIDIndexID(key)
	if err != nil {
		return "", "", 0, err
	}

	_, key, err = encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding key: %w", err)
	}
	channel = string(key)

	data, err := val.Value.GetTuple()
	if err != nil {
		return "", "", 0, err
	}

	data, payloadBs, err := encoding.DecodeBytesValue(data)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding payload: %w", err)
	}
	payload = string(payloadBs)

	_, pid64, err := encoding.DecodeIntValue(data)
	if err != nil {
		return "", "", 0, fmt.Errorf("decoding pid: %w", err)
	}
	pid = int32(pid64)

	return channel, payload, pid, nil
}
