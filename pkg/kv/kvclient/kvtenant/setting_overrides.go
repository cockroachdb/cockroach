// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvtenant

import (
	"context"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/errorspb"
)

// runTenantSettingsSubscription listens for tenant setting override changes.
// It closes the given channel once the initial set of overrides were obtained.
// Exits when the context is done.
func (c *connector) runTenantSettingsSubscription(ctx context.Context, startupCh chan<- error) {
	for ctx.Err() == nil {
		client, err := c.getClient(ctx)
		if err != nil {
			continue
		}
		stream, err := client.TenantSettings(ctx, &kvpb.TenantSettingsRequest{
			TenantID: c.tenantID,
		})
		if err != nil {
			log.Warningf(ctx, "error issuing TenantSettings RPC: %v", err)
			c.tryForgetClient(ctx, client)
			continue
		}

		// Reset the sentinel checks. We start a new sequence of messages
		// from the server every time we (re)connect.
		func() {
			c.settingsMu.Lock()
			defer c.settingsMu.Unlock()
			c.settingsMu.receivedFirstAllTenantOverrides = false
			c.settingsMu.receivedFirstSpecificOverrides = false
		}()
		func() {
			c.metadataMu.Lock()
			defer c.metadataMu.Unlock()
			c.metadataMu.receivedFirstMetadata = false
		}()

		for {
			e, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					break
				}
				// Soft RPC error. Drop client and retry.
				log.Warningf(ctx, "error consuming TenantSettings RPC: %v", err)
				c.tryForgetClient(ctx, client)
				break
			}
			if e.Error != (errorspb.EncodedError{}) {
				// Hard logical error.
				err := errors.DecodeError(ctx, e.Error)
				log.Errorf(ctx, "error consuming TenantSettings RPC: %v", err)
				if startupCh != nil && errors.Is(err, &kvpb.MissingRecordError{}) && c.earlyShutdownIfMissingTenantRecord {
					select {
					case startupCh <- err:
					case <-ctx.Done():
						// Shutdown or cancellation may prevent the receiver from getting
						// this error, so short circuit.
						return
					}

					close(startupCh)
					c.tryForgetClient(ctx, client)
					return
				}
				// Other errors, or configuration tells us to continue if the
				// tenant record in missing: in that case we continue the
				// loop. We're expecting io.EOF from the server next, which
				// will lead us to reconnect and retry.
				//
				// However, don't hammer the server with retries if there was
				// an actual error reported: we wait a bit before the retry.
				select {
				case <-time.After(1 * time.Second):

				case <-ctx.Done():
					// Shutdown or cancellation short circuits the wait and retry.
					return
				}
				continue
			}

			if c.testingEmulateOldVersionSettingsClient {
				// The old version of the settings client does not understand anything
				// else than setting events.
				e.EventType = kvpb.TenantSettingsEvent_SETTING_EVENT
			}

			var reconnect bool
			switch e.EventType {
			case kvpb.TenantSettingsEvent_METADATA_EVENT:
				err := c.processMetadataEvent(ctx, e)
				if err != nil {
					log.Errorf(ctx, "error processing tenant settings event: %v", err)
					reconnect = true
				}

			case kvpb.TenantSettingsEvent_SETTING_EVENT:
				settingsReady, err := c.processSettingsEvent(ctx, e)
				if err != nil {
					log.Errorf(ctx, "error processing tenant settings event: %v", err)
					reconnect = true
					break
				}

				// Signal that startup is complete once we have enough events
				// to start. Note: we do not connect this condition to
				// receiving the tenant metadata (via processMetadataEvent) for
				// compatibility with pre-v23.2 servers which only send
				// setting override events.
				//
				// Luckily, we are guaranteed that once we receive the setting
				// overrides the metadata has been received as well (in v23.1+
				// servers that send it) because when it is sent it is always
				// sent prior to the setting overrides.
				if settingsReady {
					log.Infof(ctx, "received initial tenant settings")

					if startupCh != nil {
						select {
						case startupCh <- nil:
						case <-ctx.Done():
							// Shutdown or cancellation may prevent the receiver from getting
							// this completion message, so short circuit.
							return
						}
						close(startupCh)
						startupCh = nil
					}
				}
			}

			if reconnect {
				_ = stream.CloseSend()
				c.tryForgetClient(ctx, client)
				break
			}
		}
	}
}

// processMetadataEvent updates the tenant metadata based on the event.
func (c *connector) processMetadataEvent(ctx context.Context, e *kvpb.TenantSettingsEvent) error {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()

	c.metadataMu.receivedFirstMetadata = true
	c.metadataMu.capabilities = e.Capabilities
	c.metadataMu.tenantName = e.Name
	// TODO(knz): Remove the cast once we have proper typing in the
	// protobuf, which requires breaking a dependency cycle.
	c.metadataMu.dataState = mtinfopb.TenantDataState(e.DataState)
	c.metadataMu.serviceMode = mtinfopb.TenantServiceMode(e.ServiceMode)
	c.metadataMu.clusterInitGracePeriodTS = e.ClusterInitGracePeriodEndTS

	log.Infof(ctx, "received tenant metadata: name=%q dataState=%v serviceMode=%v clusterInitGracePeriodTS=%s\ncapabilities=%+v",
		c.metadataMu.tenantName, c.metadataMu.dataState, c.metadataMu.serviceMode,
		timeutil.Unix(c.metadataMu.clusterInitGracePeriodTS, 0), c.metadataMu.capabilities)

	// Signal watchers that there was an update.
	close(c.metadataMu.notifyCh)
	c.metadataMu.notifyCh = make(chan struct{})

	return nil
}

// TenantInfo accesses the tenant metadata.
func (c *connector) TenantInfo() (tenantcapabilities.Entry, <-chan struct{}) {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()

	return tenantcapabilities.Entry{
		TenantID:           c.tenantID,
		TenantCapabilities: c.metadataMu.capabilities,
		Name:               c.metadataMu.tenantName,
		DataState:          c.metadataMu.dataState,
		ServiceMode:        c.metadataMu.serviceMode,
	}, c.metadataMu.notifyCh
}

// ReadFromTenantInfo allows retrieving the other tenant, if any, from which the
// calling tenant should configure itself to read, along with the latest
// timestamp at which it should perform such reads at this time.
func (c *connector) ReadFromTenantInfo(
	ctx context.Context,
) (roachpb.TenantID, hlc.Timestamp, error) {
	if c.tenantID.IsSystem() {
		return roachpb.TenantID{}, hlc.Timestamp{}, nil
	}

	client, err := c.getClient(ctx)
	if err != nil {
		return roachpb.TenantID{}, hlc.Timestamp{}, err
	}
	resp, err := client.ReadFromTenantInfo(ctx, &serverpb.ReadFromTenantInfoRequest{TenantID: c.tenantID})
	if err != nil {
		return roachpb.TenantID{}, hlc.Timestamp{}, err
	}
	return resp.ReadFrom, resp.ReadAt, nil
}

// processSettingsEvent updates the setting overrides based on the event.
func (c *connector) processSettingsEvent(
	ctx context.Context, e *kvpb.TenantSettingsEvent,
) (settingsReady bool, err error) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	var m map[settings.InternalKey]settings.EncodedValue
	switch e.Precedence {
	case kvpb.TenantSettingsEvent_ALL_TENANTS_OVERRIDES:
		if !c.settingsMu.receivedFirstAllTenantOverrides && e.Incremental {
			return false, errors.Newf(
				"need to receive non-incremental setting event first for precedence %v",
				e.Precedence,
			)
		}

		c.settingsMu.receivedFirstAllTenantOverrides = true
		m = c.settingsMu.allTenantOverrides

	case kvpb.TenantSettingsEvent_TENANT_SPECIFIC_OVERRIDES:
		if !c.settingsMu.receivedFirstSpecificOverrides && e.Incremental {
			return false, errors.Newf(
				"need to receive non-incremental setting events first for precedence %v",
				e.Precedence,
			)
		}
		c.settingsMu.receivedFirstSpecificOverrides = true
		m = c.settingsMu.specificOverrides

	default:
		return false, errors.Newf("unknown precedence value %d", e.Precedence)
	}

	log.Infof(ctx, "received %d setting overrides with precedence %v (incremental=%v)", len(e.Overrides), e.Precedence, e.Incremental)

	// If the event is not incremental, clear the map.
	if !e.Incremental {
		for k := range m {
			delete(m, k)
		}
	}
	// Merge in the override changes.
	for _, o := range e.Overrides {
		if o.Value == (settings.EncodedValue{}) {
			// Empty value indicates that the override is removed.
			log.VEventf(ctx, 1, "removing %v override for %q", e.Precedence, o.InternalKey)
			delete(m, o.InternalKey)
		} else {
			log.VEventf(ctx, 1, "adding %v override for %q = %q", e.Precedence, o.InternalKey, o.Value.Value)
			m[o.InternalKey] = o.Value
		}
	}

	// Notify watchers if any.
	close(c.settingsMu.notifyCh)
	// Define a new notification channel for subsequent watchers.
	c.settingsMu.notifyCh = make(chan struct{})

	// The protocol defines that the server sends one initial
	// non-incremental message for both precedences.
	settingsReady = c.settingsMu.receivedFirstAllTenantOverrides && c.settingsMu.receivedFirstSpecificOverrides
	return settingsReady, nil
}

// Overrides is part of the settingswatcher.OverridesMonitor interface.
func (c *connector) Overrides() (map[settings.InternalKey]settings.EncodedValue, <-chan struct{}) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	res := make(map[settings.InternalKey]settings.EncodedValue, len(c.settingsMu.allTenantOverrides)+len(c.settingsMu.specificOverrides))

	// First copy the all-tenant overrides.
	for key, val := range c.settingsMu.allTenantOverrides {
		res[key] = val
	}
	// Then copy the specific overrides (which can overwrite some all-tenant
	// overrides).
	for key, val := range c.settingsMu.specificOverrides {
		res[key] = val
	}
	return res, c.settingsMu.notifyCh
}

func (c *connector) GetClusterInitGracePeriodTS() int64 {
	c.metadataMu.Lock()
	defer c.metadataMu.Unlock()
	return c.metadataMu.clusterInitGracePeriodTS
}
