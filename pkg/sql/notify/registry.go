// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package notify

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
)

// RangefeedEnabled is a cluster setting that enables rangefeed requests.
var channelQueueSize = settings.RegisterIntSetting(
	settings.TenantReadOnly,
	"sql.pg_notifications.max_pending",
	"the number of outstanding notifications permitted per channel per session before new notifications are dropped",
	1024,
)

// Payload is a NOTIFY payload, including the channel the notification is sent
// on, the message being sent, and the ID of the sending session.
type Payload struct {
	ChannelName string
	Message     string
	NodeID      int32
}

// NotificationSender is an object that can send a notification to a client.
type NotificationSender interface {
	// SendNotification sends the notification payload to the client.
	SendNotification(ctx context.Context, payload Payload)
}

// ListenersRegistry manages the set of listeners for the LISTEN/NOTIFY
// functionality.
type ListenersRegistry struct {
	log.AmbientContext

	settings *cluster.Settings
	clock    *hlc.Clock
	stopper  *stop.Stopper
	sender   *kvcoord.DistSender
	mu       struct {
		syncutil.Mutex
		sessionsToChannels map[uint128.Uint128]map[string]chan Payload
		channelsToSessions map[string]map[uint128.Uint128]chan Payload
	}
}

// NewRegistry creates a new ListenersRegistry, but doesn't start it.
func NewRegistry(
	settings *cluster.Settings, stopper *stop.Stopper, clock *hlc.Clock, sender *kvcoord.DistSender,
) *ListenersRegistry {
	var r ListenersRegistry
	r.settings = settings
	r.mu.sessionsToChannels = make(map[uint128.Uint128]map[string]chan Payload)
	r.mu.channelsToSessions = make(map[string]map[uint128.Uint128]chan Payload)
	r.stopper = stopper
	r.clock = clock
	r.sender = sender
	return &r
}

// Listen registers a listener for the given session ID and channel
// name, returning the channel that the listener should read on for events.
// The returned channel will be closed if the user UNLISTENs it, or logs out.
func (r *ListenersRegistry) Listen(sessionID uint128.Uint128, channelName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if chanMap := r.mu.sessionsToChannels[sessionID]; chanMap != nil {
		if chanMap[channelName] != nil {
			// We're already listening on this channel.
			return
		}
	}
	ch := make(chan Payload, channelQueueSize.Get(&r.settings.SV))
	{
		chanMap := r.mu.sessionsToChannels[sessionID]
		if chanMap == nil {
			chanMap = make(map[string]chan Payload)
			r.mu.sessionsToChannels[sessionID] = chanMap
		}
		chanMap[channelName] = ch
	}
	{
		sessMap := r.mu.channelsToSessions[channelName]
		if sessMap == nil {
			sessMap = make(map[uint128.Uint128]chan Payload)
			r.mu.channelsToSessions[channelName] = sessMap
		}
		sessMap[sessionID] = ch
	}
}

// Unlisten unregisters a listener for the given session ID and
// channel name.
func (r *ListenersRegistry) Unlisten(sessionID uint128.Uint128, channelName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if chanMap := r.mu.sessionsToChannels[sessionID]; chanMap != nil {
		if ch := chanMap[channelName]; ch != nil {
			close(ch)
			delete(chanMap, channelName)
			if len(chanMap) == 0 {
				delete(r.mu.sessionsToChannels, sessionID)
			}
		}
	}
	r.deleteFromChannelsToSessionsLocked(sessionID, channelName)
}

// UnlistenAll unregisters all of the listeners for the given session ID.
func (r *ListenersRegistry) UnlistenAll(sessionID uint128.Uint128) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if chanMap := r.mu.sessionsToChannels[sessionID]; chanMap != nil {
		for channelName, ch := range chanMap {
			r.deleteFromChannelsToSessionsLocked(sessionID, channelName)
			close(ch)
		}
		delete(r.mu.sessionsToChannels, sessionID)
	}
}

func (r *ListenersRegistry) deleteFromChannelsToSessionsLocked(
	sessionID uint128.Uint128, channelName string,
) {
	if sessMap := r.mu.channelsToSessions[channelName]; sessMap != nil {
		delete(sessMap, sessionID)
		if len(sessMap) == 0 {
			delete(r.mu.channelsToSessions, channelName)
		}
	}
}

// HasListeners returns whether or not chanName had any listeners at the time of
// of calling.
func (r *ListenersRegistry) HasListeners(chanName string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.mu.channelsToSessions[chanName]) > 0
}

// Notify notifies all listeners on channel chanName with the given payload.
func (r *ListenersRegistry) Notify(ctx context.Context, chanName string, payload Payload) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, listener := range r.mu.channelsToSessions[chanName] {
		log.VEventf(ctx, 2, "Forwarding notification to session %s", id)
		select {
		case listener <- payload:
		default:
			log.Warningf(ctx, "Dropping NOTIFY event due to full queue on chan %s", chanName)
		}
	}
}

// DrainNotifications returns all notifications received for the input sessionID
// and removes them from the notification channels.
func (r *ListenersRegistry) DrainNotifications(
	ctx context.Context, sessionID uint128.Uint128,
) []Payload {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ret []Payload
	for chanName, ch := range r.mu.sessionsToChannels[sessionID] {
		found := false
		for len(ch) > 0 {
			if !found {
				log.VEventf(ctx, 2, "Draining notifications on session %s chan %s", sessionID, chanName)
				found = true
			}
			ret = append(ret, <-ch)
		}
	}
	return ret
}

// Start starts this registry using the given stopper to control its lifetime.
// This method returns immediately, and its goroutines will stop once the
// stopper quiesces.
func (r *ListenersRegistry) Start(
	ctx context.Context, ie sqlutil.InternalExecutor, codec keys.SQLCodec,
) {
	// Make span that contains just pg_notifications table.
	ch := make(chan *roachpb.RangeFeedEvent, 1024)
	ctx, _ = r.stopper.WithCancelOnQuiesce(ctx)
	if !r.settings.Version.IsActive(ctx, clusterversion.PGNotificationsTable) {
		timer := timeutil.NewTimer()
		timer.Reset(0)
		for {
			select {
			case <-timer.C:
				timer.Read = true
				if r.settings.Version.IsActive(ctx, clusterversion.PGNotificationsTable) {
					break
				}
				timer.Reset(10 * time.Second)
			case <-ctx.Done():
				return
			case <-r.stopper.ShouldQuiesce():
				return
			}
		}
	}
	// Fetch the pg_notifications table ID.
	res, err := ie.QueryRowEx(ctx, "fetch-pg-notification-table-id", nil, sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(`SELECT 'system.%s'::regclass::oid`, catconstants.PGNotificationsTableName))
	if err != nil {
		log.Warningf(ctx, "pg_notifications table missing?", err)
		return
	}
	oid := tree.MustBeDOid(res[0])
	id := uint32(oid.Oid)
	prefix := codec.TablePrefix(id)
	notificationsTableSpan := roachpb.Span{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
	}

	now := r.clock.Now()
	if err := r.stopper.RunAsyncTask(ctx, "pg_notifications rangefeed", func(ctx context.Context) {
		for {
			err := r.sender.RangeFeed(ctx,
				[]roachpb.Span{notificationsTableSpan},
				now,
				false, /* withDiff */
				ch,
			)
			if err != nil && ctx.Err() == nil {
				log.Warningf(ctx, "pg_notifications rangefeed failed, restarting: %v", err)
			}
			if ctx.Err() != nil {
				log.VEvent(ctx, 1, "exiting pg_notifications rangefeed")
				return
			}
		}
	}); err != nil {
		// This only happens if we were stopped/quiesced ahead of time.
		log.Errorf(ctx, "exiting pg_notifications rangefeed: %v", err)
	}
	if err := r.stopper.RunAsyncTask(ctx, "pg_notifications bus", func(ctx context.Context) {
		for {
			select {
			case rfEvent := <-ch:
				if rfEvent.Val != nil {
					// First, we'll have to decode the key of our payload into a pretty
					// channel name.
					key := rfEvent.Val.Key
					key, _, err = keys.DecodeTenantPrefix(key)
					if err != nil {
						log.Errorf(ctx, "Failed to decode tenantid from channel %s", rfEvent.Val.Key)
						continue
					}
					key, _, _, err := keys.DecodeTableIDIndexID(key)
					if err != nil {
						log.Errorf(ctx, "Failed to decode tableid/indexid from channel %s", rfEvent.Val.Key)
						continue
					}
					_, chanNameBytes, err := encoding.DecodeBytesAscending(key, nil)
					if err != nil {
						log.Errorf(ctx, "Failed to decode channel name from channel %s", rfEvent.Val.Key)
						continue
					}

					chanName := string(chanNameBytes)
					// Short circuit decoding the rest of the payload if there are no
					// listeners on the channel. This is racy - if somebody connects after
					// this line, they'll miss the message. But I think that's okay.
					if !r.HasListeners(chanName) {
						continue
					}

					// Decode the value bytes.
					val, err := rfEvent.Val.Value.GetTuple()
					if err != nil {
						log.Errorf(ctx, "Failed to decode tuple %s", rfEvent.Val.Value)
						continue
					}
					val, message, err := encoding.DecodeBytesValue(val)
					if err != nil {
						log.Errorf(ctx, "Failed to decode message %s", rfEvent.Val.Value)
						continue
					}
					_, nodeID, err := encoding.DecodeIntValue(val)
					if err != nil {
						log.Errorf(ctx, "Failed to decode nodeID %s", rfEvent.Val.Value)
						continue
					}
					notificationPayload := Payload{
						ChannelName: chanName,
						Message:     string(message),
						NodeID:      int32(nodeID),
					}
					// Notify all listeners on the channel.
					r.Notify(ctx, chanName, notificationPayload)
				}
			case <-ctx.Done():
				return
			}
		}
	}); err != nil {
		log.Errorf(ctx, "exiting pg_notifications rangefeed: %v", err)
	}
}
