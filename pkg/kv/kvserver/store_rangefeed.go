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

func (r rangeFeedUpdaterConf) getRefresh() time.Duration {
	refresh := RangeFeedRefreshInterval.Get(&r.settings.SV)
	if refresh <= 0 {
		refresh = closedts.SideTransportCloseInterval.Get(&r.settings.SV)
	}
	return refresh
}

func (r rangeFeedUpdaterConf) getSmear() time.Duration {
	refresh := r.getRefresh()
	smear := RangeFeedSmearInterval.Get(&r.settings.SV)
	if smear <= 0 || smear > refresh {
		smear = refresh
	}
	return smear
}
