// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/viper"
)

type viperDenyList struct {
	pollInterval time.Duration
	mu           struct {
		sync.Mutex
		viperCfg *viper.Viper
	}
}

// NewViperDenyListFromFile returns a new denylist Service backed by a local
// file using https://github.com/spf13/viper.
func NewViperDenyListFromFile(
	ctx context.Context, file string, pollConfigInterval time.Duration,
) (Service, error) {
	d := &viperDenyList{pollInterval: pollConfigInterval}
	if file != "" {
		if vprCfg, err := newViperCfgFromFile(file); err != nil {
			return nil, err
		} else {
			log.Infof(ctx, "current denied keys: %+v", vprCfg.AllKeys())
			d.mu.viperCfg = vprCfg
		}
		d.watchForUpdates(ctx)
	}

	return d, nil
}

// newViperCfgFromFile creates a new viper instance from a local file.
func newViperCfgFromFile(cfgFileName string) (*viper.Viper, error) {
	v := viper.New()
	v.SetConfigFile(cfgFileName)
	if err := v.ReadInConfig(); err != nil {
		return nil, errors.Wrap(err, "could not read denylist file")
	}
	log.Infof(context.Background(), "successfully loaded denylist file %s", cfgFileName)
	return v, nil
}

// Denied implements the Service interface using viper to query the deny list.
func (d *viperDenyList) Denied(item string) (*Entry, error) {
	if d.mu.viperCfg == nil {
		return nil, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deniedLocked(item)
}

func (d *viperDenyList) deniedLocked(item string) (*Entry, error) {
	if msg := d.mu.viperCfg.Get(item); msg != nil {
		return &Entry{Reason: msg.(string)}, nil
	}
	return nil, nil
}

// WatchForUpdates periodically reloads the denylist file. The daemon is
// canceled on ctx cancellation.
// TODO(spaskob): use notification via SIGHUP instead or use the viper API
// WatchConfig.
func (d *viperDenyList) watchForUpdates(ctx context.Context) {
	go func() {
		t := timeutil.NewTimer()
		defer t.Stop()
		for {
			t.Reset(util.Jitter(d.pollInterval, 0.15 /* fraction */))
			select {
			case <-ctx.Done():
				log.Errorf(ctx, "WatchList daemon stopped: %v", ctx.Err())
				return
			case <-t.C:
				t.Read = true
				d.mu.Lock()
				if err := d.mu.viperCfg.ReadInConfig(); err != nil {
					// Only log the error since it may happen that the file is
					// being written to or replaced at the same time.
					log.Errorf(ctx, "could not read denylist %s: %v", d.mu.viperCfg.ConfigFileUsed(), err)
				}
				d.mu.Unlock()
			}
		}
	}()
}
