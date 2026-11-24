// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package operations

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operations/changefeeds"
)

func TestParseConfigs(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
		wantMap map[string]int
	}{
		{
			name:    "valid config",
			config:  "kafka:20,webhook:30,null:50",
			wantErr: false,
			wantMap: map[string]int{"kafka": 20, "webhook": 30, "null": 50},
		},
		{
			name:    "percentage does not add up to 100",
			config:  "kafka:20,webhook:30,null:40",
			wantErr: true,
		},
		{
			name:    "invalid percentage value",
			config:  "kafka:abc,webhook:30,null:70",
			wantErr: true,
		},
		{
			name:    "missing colon",
			config:  "kafka20,webhook:30,null:70",
			wantErr: true,
		},
		{
			name:    "negative percentage",
			config:  "kafka:-10,webhook:60,null:50",
			wantErr: true,
		},
		{
			name:    "empty config",
			config:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMap := make(map[string]int)
			err := changefeeds.ParseConfigs(tt.config, func(key string, value int) error {
				gotMap[key] = value
				return nil
			})

			if (err != nil) != tt.wantErr {
				t.Errorf("parseConfigs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !equalMaps(gotMap, tt.wantMap) {
				t.Errorf("parseConfigs() gotMap = %v, wantMap %v", gotMap, tt.wantMap)
			}
		})
	}
}

func equalMaps(a, b map[string]int) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
