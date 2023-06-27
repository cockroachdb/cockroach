// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvtenant

import (
	"context"
	"io"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
				// Hard logical error. We expect io.EOF next.
				err := errors.DecodeError(ctx, e.Error)
				log.Errorf(ctx, "error consuming TenantSettings RPC: %v", err)
				if startupCh != nil {
					startupCh <- err
					close(startupCh)
					return
				}
				continue
			}

			settingsReady, err := c.processSettingsEvent(e)
			if err != nil {
				log.Errorf(ctx, "error processing tenant settings event: %v", err)
				_ = stream.CloseSend()
				c.tryForgetClient(ctx, client)
				break
			}

			// Signal that startup is complete once we have enough events to start.
			if settingsReady {
				log.Infof(ctx, "received initial tenant settings")

				if startupCh != nil {
					startupCh <- nil
					close(startupCh)
					startupCh = nil
				}
			}
		}
	}
}

// processSettingsEvent updates the setting overrides based on the event.
func (c *connector) processSettingsEvent(
	e *kvpb.TenantSettingsEvent,
) (settingsReady bool, err error) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	var m map[string]settings.EncodedValue
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
			delete(m, o.Name)
		} else {
			m[o.Name] = o.Value
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
func (c *connector) Overrides() (map[string]settings.EncodedValue, <-chan struct{}) {
	c.settingsMu.Lock()
	defer c.settingsMu.Unlock()

	res := make(map[string]settings.EncodedValue, len(c.settingsMu.allTenantOverrides)+len(c.settingsMu.specificOverrides))

	// First copy the all-tenant overrides.
	for name, val := range c.settingsMu.allTenantOverrides {
		res[name] = val
	}
	// Then copy the specific overrides (which can overwrite some all-tenant
	// overrides).
	for name, val := range c.settingsMu.specificOverrides {
		res[name] = val
	}
	return res, c.settingsMu.notifyCh
}
