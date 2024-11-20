package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotification"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type listenerID clusterunique.ID

type NotificationSender interface {
	SendNotification(payload pgnotification.Notification) error
}

type listenerHandle struct {
	cancel context.CancelFunc
	eg     *errgroup.Group
	done   *atomic.Int64
}

type ListenerRegistry struct {
	stopper    *stop.Stopper
	settings   *cluster.Settings
	timeSource timeutil.TimeSource
	ie         *InternalExecutor

	handles struct {
		syncutil.Mutex
		m map[listenerID]map[string]listenerHandle
	}
}

func NewListenerRegistry(
	stopper *stop.Stopper,
	settings *cluster.Settings,
	// timeSource timeutil.TimeSource,
	ie *InternalExecutor,
) *ListenerRegistry {
	r := &ListenerRegistry{
		stopper:    stopper,
		settings:   settings,
		timeSource: timeutil.DefaultTimeSource{},
		ie:         ie,
	}
	r.handles.m = make(map[listenerID]map[string]listenerHandle)
	return r
}

func (r *ListenerRegistry) StartListener(
	ctx context.Context,
	id listenerID,
	channel string,
	sender NotificationSender,
	sessionData *sessiondatapb.SessionData,
	statementTime hlc.Timestamp,
	planner PlanHookState,
) error {
	ctx, _ = r.stopper.WithCancelOnQuiesce(ctx) // this ctx is passed to spawned tasks so dont cancel it

	// if the table doesnt exist, wait until it does
	if !r.settings.Version.IsActive(ctx, clusterversion.V25_1_ListenNotifyQueue) {
		ticker := r.timeSource.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.Ch():
			}
			if r.settings.Version.IsActive(ctx, clusterversion.V25_1_ListenNotifyQueue) {
				break
			}
		}
	}

	// Get the notifications table's id. TODO: do in a sync.Once
	res, err := startup.RunIdempotentWithRetryEx(ctx, r.stopper.ShouldQuiesce(), "fetch-pg-notification-table-id", func(ctx context.Context) (tree.Datums, error) {
		return r.ie.QueryRowEx(ctx, "fetch-pg-notification-table-id", nil, sessiondata.NodeUserSessionDataOverride,
			fmt.Sprintf(`SELECT 'system.%s'::regclass::oid`, catconstants.ListenNotifyQueueTableName))
	})
	if err != nil {
		return err
	}
	tableID := tree.MustBeDOid(res[0]).Oid

	details := jobspb.ChangefeedDetails{
		Tables: map[catid.DescID]jobspb.ChangefeedTargetTable{
			catid.DescID(tableID): {
				StatementTimeName: "system.notifications",
			},
		},
		Opts: map[string]string{
			"initial_scan": "no",
			"envelope":     "row",
		},
		StatementTime: statementTime,
		TargetSpecifications: []jobspb.ChangefeedTargetSpecification{
			{
				Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
				TableID:           catid.DescID(tableID),
				StatementTimeName: "system.notifications",
			},
		},
		SessionData: sessionData,
		// TODO: fix sql injection
		Select: fmt.Sprintf("SELECT channel, payload, pid FROM system.notifications WHERE channel = '%s'", channel),
	}
	progress := jobspb.Progress{
		Progress: &jobspb.Progress_HighWater{},
		Details: &jobspb.Progress_Changefeed{
			Changefeed: &jobspb.ChangefeedProgress{},
		},
	}

	func() {
		r.handles.Lock()
		defer r.handles.Unlock()

		if m, ok := r.handles.m[id]; ok {
			if handle, ok := m[channel]; ok {
				if handle.done.Load() < 2 {
					return
				}
			}
		}

		childCtx := context.WithoutCancel(ctx) // disconnect cancellation from caller (?)
		childCtx, cancel := r.stopper.WithCancelOnQuiesce(childCtx)
		eg, childCtx := errgroup.WithContext(childCtx)
		var doneInt atomic.Int64

		resultsCh := make(chan tree.Datums, 1024)
		eg.Go(func() error {
			defer doneInt.Add(1)
			for {
				select {
				case <-childCtx.Done():
					return childCtx.Err()
				case <-r.stopper.ShouldQuiesce():
					return nil
				case row, ok := <-resultsCh:
					if !ok {
						return nil
					}
					msg := tree.MustBeDBytes(row[2])
					var rowMsg rowMsg
					if err := json.Unmarshal([]byte(msg), &rowMsg); err != nil {
						return errors.Wrap(err, "failed to unmarshal row")
					}

					notif := pgnotification.Notification{
						Channel: rowMsg.Channel,
						Payload: rowMsg.Payload,
						PID:     int32(rowMsg.PID),
					}
					sender.SendNotification(notif)
				}
			}
		})
		eg.Go(func() error {
			defer close(resultsCh)
			defer doneInt.Add(1)
			err := StartCoreChangefeed(childCtx, planner, details, progress, resultsCh)
			if err != nil {
				log.Warningf(childCtx, "listen/notify changefeed error: %v", err)
			}
			return err
		})

		if _, ok := r.handles.m[id]; !ok {
			r.handles.m[id] = make(map[string]listenerHandle)
		}
		r.handles.m[id][channel] = listenerHandle{cancel: cancel, eg: eg, done: &doneInt}
	}()

	return nil
}

func (r *ListenerRegistry) RemoveListener(ctx context.Context, id listenerID, channel string) {
	r.handles.Lock()
	defer r.handles.Unlock()

	if log.V(2) {
		log.Infof(ctx, "removing listener for %d on %s", id, channel)
	}

	if m, ok := r.handles.m[id]; ok {
		if handle, ok := m[channel]; ok {
			handle.cancel()
			_ = handle.eg.Wait()
			delete(r.handles.m, id)
		}
	}
}

func (r *ListenerRegistry) RemoveAllListeners(ctx context.Context, id listenerID) {
	r.handles.Lock()
	defer r.handles.Unlock()

	if log.V(2) {
		log.Infof(ctx, "removing all listeners for %d", id)
	}

	if m, ok := r.handles.m[id]; ok {
		for _, handle := range m {
			handle.cancel()
			_ = handle.eg.Wait()
		}
		delete(r.handles.m, id)
	}
}

type rowMsg struct {
	Channel string `json:"channel"`
	Payload string `json:"payload"`
	PID     int    `json:"pid"`
}

var StartCoreChangefeed func(
	ctx context.Context,
	p PlanHookState,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
) error
