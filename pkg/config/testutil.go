// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type zoneConfigMap map[ObjectID]zonepb.ZoneConfig

var (
	testingZoneConfig   zoneConfigMap
	testingHasHook      bool
	testingPreviousHook zoneConfigHook
	testingLock         syncutil.Mutex
)

// TestingSetupZoneConfigHook initializes the zone config hook
// to 'testingZoneConfigHook' which uses 'testingZoneConfig'.
// Settings go back to their previous values when the stopper runs our closer.
func TestingSetupZoneConfigHook(stopper *stop.Stopper) {
	stopper.AddCloser(stop.CloserFn(testingResetZoneConfigHook))

	testingLock.Lock()
	defer testingLock.Unlock()
	if testingHasHook {
		panic("TestingSetupZoneConfigHook called without restoring state")
	}
	testingHasHook = true
	testingZoneConfig = make(zoneConfigMap)
	testingPreviousHook = ZoneConfigHook
	ZoneConfigHook = testingZoneConfigHook
	testingLargestIDHook = func(maxID ObjectID) (max ObjectID) {
		testingLock.Lock()
		defer testingLock.Unlock()
		for id := range testingZoneConfig {
			if maxID != 0 && maxID < id {
				continue
			}
			if id > max {
				max = id
			}
		}
		return
	}
}

// testingResetZoneConfigHook resets the zone config hook back to what it was
// before TestingSetupZoneConfigHook was called.
func testingResetZoneConfigHook() {
	testingLock.Lock()
	defer testingLock.Unlock()
	if !testingHasHook {
		panic("TestingResetZoneConfigHook called on uninitialized testing hook")
	}
	testingHasHook = false
	ZoneConfigHook = testingPreviousHook
	testingLargestIDHook = nil
}

// TestingSetZoneConfig sets the zone config entry for object 'id'
// in the testing map.
func TestingSetZoneConfig(id ObjectID, zone zonepb.ZoneConfig) {
	testingLock.Lock()
	defer testingLock.Unlock()
	testingZoneConfig[id] = zone
}

func testingZoneConfigHook(
	_ *SystemConfig, codec keys.SQLCodec, id ObjectID,
) (*zonepb.ZoneConfig, *zonepb.ZoneConfig, bool, error) {
	testingLock.Lock()
	defer testingLock.Unlock()
	if zone, ok := testingZoneConfig[id]; ok {
		return &zone, nil, false, nil
	}
	return nil, nil, false, nil
}
