package notify

import (
	"context"
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
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
	"enable notifications in the server/client protocol being sent",
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

	// TODO: is it cool to overwrite this if one exists already?
	cm.senders[id] = s
}

func (cm *channelMux) RemoveSender(id ListenerID) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.senders, id)
}

// RunBatch reads a chunk of messages from the channel and sends them to the senders.
func (cm *channelMux) RunBatch(ctx context.Context) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// fill up buf with up to batchSize messages
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

	for _, n := range cm.batchBuf {
		for _, s := range cm.senders {
			// TODO:
			// - guard against slow clients
			// - handle errors (poison connection? remove listener?)
			if err := s.SendNotification(n); err != nil {
				log.Warningf(ctx, "error sending notification: %v", err)
			}
		}
	}
}

type queryRowExer interface {
	QueryRowEx(ctx context.Context,
		opName string,
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

	timeSource timeutil.TimeSource

	listenersMu struct {
		syncutil.Mutex
		channels map[string]*channelMux
	}
}

func NewRegistry(
	settings *cluster.Settings,
	stopper *stop.Stopper,
	sender *kvcoord.DistSender,
	clock *hlc.Clock,
	ie queryRowExer,
) *ListenerRegistry {
	r := &ListenerRegistry{
		settings:   settings,
		stopper:    stopper,
		sender:     sender,
		clock:      clock,
		ie:         ie,
		timeSource: timeutil.DefaultTimeSource{},
	}
	r.listenersMu.channels = make(map[string]*channelMux)
	return r
}

func (r *ListenerRegistry) AddListener(ctx context.Context, id ListenerID, channel string, sender NotificationSender) {
	if !notificationsEnabled.Get(&r.settings.SV) {
		// TODO: send a notice?
		return
	}

	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if _, ok := r.listenersMu.channels[channel]; !ok {
		r.listenersMu.channels[channel] = &channelMux{
			senders: make(map[ListenerID]NotificationSender, 1),
			ch:      make(chan pgnotification.Notification, channelQueueSize.Get(&r.settings.SV)),
		}
	}
	r.listenersMu.channels[channel].AddSender(id, sender)
}

func (r *ListenerRegistry) RemoveListener(ctx context.Context, id ListenerID, channel string) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if cm, ok := r.listenersMu.channels[channel]; ok {
		cm.RemoveSender(id)
		if len(cm.senders) == 0 {
			delete(r.listenersMu.channels, channel)
		}
	}
}

func (r *ListenerRegistry) RemoveAllListeners(ctx context.Context, id ListenerID) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	for channel, cm := range r.listenersMu.channels {
		cm.RemoveSender(id)
		if len(cm.senders) == 0 {
			delete(r.listenersMu.channels, channel)
		}
	}
}

func (r *ListenerRegistry) Notify(ctx context.Context, channel string, payload string, pid int32) {
	r.listenersMu.Lock()
	defer r.listenersMu.Unlock()

	if cm, ok := r.listenersMu.channels[channel]; ok {
		select {
		case cm.ch <- pgnotification.Notification{Channel: channel, Payload: payload, PID: pid}:
		default:
			log.Warningf(ctx, "dropping notification on channel %s", channel)
		}
	}
}

func (r *ListenerRegistry) Start(ctx context.Context) {
	r.stopper.RunAsyncTask(ctx, "listener_registry_run_batches", r.runBatches)
	r.startRangefeed(ctx)
}

const tickIvl = 100 * time.Millisecond

// call channelMux.RunBatch on all channelMuxes
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

		func() {
			r.listenersMu.Lock()
			defer r.listenersMu.Unlock()

			cms = cms[:0]
			for _, cm := range r.listenersMu.channels {
				cms = append(cms, cm)
			}
		}()

		// TODO: do this with some constant parallelism
		for _, cm := range cms {
			cm.RunBatch(ctx)
		}
	}
}

func (r *ListenerRegistry) startRangefeed(ctx context.Context) {
	ctx, _ = r.stopper.WithCancelOnQuiesce(ctx) // this ctx is passed to spawned tasks so dont cancel it

	// if the table doesnt exist, wait until it does
	if !r.settings.Version.IsActive(ctx, clusterversion.V24_3_ListenNotifyQueue) {
		ticker := r.timeSource.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Ch():
			}
			if r.settings.Version.IsActive(ctx, clusterversion.V24_3_ListenNotifyQueue) {
				break
			}
		}
	}

	// get notifications table span
	res, err := r.ie.QueryRowEx(ctx, "fetch-pg-notification-table-id", nil, sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT 'system.%s'::regclass::oid`, catconstants.ListenNotifyQueueTableName))
	if err != nil {
		log.Warningf(ctx, "pg_notifications table missing?", err)
		return
	}
	oid := tree.MustBeDOid(res[0])
	prefix := keys.SystemSQLCodec.TablePrefix(uint32(oid.Oid))
	spans := []roachpb.Span{{Key: prefix, EndKey: prefix.PrefixEnd()}}
	ch := make(chan kvcoord.RangeFeedMessage, 1024)
	startTs := r.clock.Now()

	// run the rangefeed in a goroutine, retrying forever
	err = r.stopper.RunAsyncTask(ctx, "listener_registry_rangefeed", func(ctx context.Context) {
		for {
			if err := r.sender.RangeFeed(ctx, spans, startTs, ch); err != nil {
				log.Warningf(ctx, "notifications rangefeed failed: %v", err)
				if ctx.Err() != nil {
					return
				}
			}
		}
	})
	if err != nil {
		log.Warningf(ctx, "failed to run rangefeed: %v", err)
		return
	}

	// feed the rangefeed messages into the registry
	err = r.stopper.RunAsyncTask(ctx, "listener_registry_rangefeed_consumer", func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				if msg.Error != nil {
					// ?
					continue
				}
				val := msg.Val
				if val == nil {
					// ?
					continue
				}

				channel, payload, pid, err := decodeRangefeedVal(val)
				if err != nil {
					log.Warningf(ctx, "failed to decode rangefeed msg: %v", err)
					continue
				}

				r.Notify(ctx, channel, payload, pid)
			}
		}
	})
	if err != nil {
		log.Warningf(ctx, "failed to run rangefeed consumer: %v", err)
		return
	}
}

func decodeRangefeedVal(val *kvpb.RangeFeedValue) (channel, payload string, pid int32, err error) {
	key, _, _ := keys.DecodeTenantPrefix(val.Key)
	key, _, _, err = keys.DecodeTableIDIndexID(key)
	if err != nil {
		return "", "", 0, err
	}

	_, key, err = encoding.DecodeBytesAscending(key, nil)
	if err != nil {
		return "", "", 0, err
	}
	channel = string(key)

	data, err := val.Value.GetTuple()
	if err != nil {
		return "", "", 0, err
	}

	data, payloadBs, err := encoding.DecodeBytesValue(data)
	if err != nil {
		return "", "", 0, err
	}
	payload = string(payloadBs)

	data, pid64, err := encoding.DecodeIntValue(data)
	if err != nil {
		return "", "", 0, err
	}
	pid = int32(pid64)

	return channel, payload, pid, nil
}
