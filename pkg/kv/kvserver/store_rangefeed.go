// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// rangeFeedUpdaterConf provides configuration for the rangefeed updater job,
// and allows watching for when it is updated.
// rangeFeedUpdaterConf Implements the taskPacerConfig interface.
type rangeFeedUpdaterConf struct {
	settings *cluster.Settings
	changed  <-chan struct{}
}

// newRangeFeedUpdaterConf creates the config reading from and watching the
// given cluster settings.
func newRangeFeedUpdaterConf(st *cluster.Settings) rangeFeedUpdaterConf {
	confCh := make(chan struct{}, 1)
	confChanged := func(ctx context.Context) {
		select {
		case confCh <- struct{}{}:
		default:
		}
	}
	closedts.SideTransportCloseInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedRefreshInterval.SetOnChange(&st.SV, confChanged)
	RangeFeedSmearInterval.SetOnChange(&st.SV, confChanged)
	return rangeFeedUpdaterConf{settings: st, changed: confCh}
}

// wait blocks until it receives a valid rangefeed closed timestamp pacing
// configuration, and returns it.
func (r rangeFeedUpdaterConf) wait(ctx context.Context) error {
	for {
		refresh := r.getRefresh()
		smear := r.getSmear()
		if refresh != 0 && smear != 0 {
			return nil
		}
		select {
		case <-r.changed:
			// Loop back around and check if the config is good now.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// getRefresh returns the refresh interval for the rangefeed updater.
func (r rangeFeedUpdaterConf) getRefresh() time.Duration {
	refresh := RangeFeedRefreshInterval.Get(&r.settings.SV)
	if refresh <= 0 {
		refresh = closedts.SideTransportCloseInterval.Get(&r.settings.SV)
	}
	return refresh
}

// getSmear returns the smear interval for the rangefeed updater.
func (r rangeFeedUpdaterConf) getSmear() time.Duration {
	refresh := r.getRefresh()
	smear := RangeFeedSmearInterval.Get(&r.settings.SV)
	if smear <= 0 || smear > refresh {
		smear = refresh
	}
	return smear
}
