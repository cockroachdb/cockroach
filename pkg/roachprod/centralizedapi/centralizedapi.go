// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package centralizedapi

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/client"
)

var (
	once           sync.Once
	centralizedapi *client.Client
)

// GetCentralizedAPIClient returns a singleton instance of the centralized API client.
func GetCentralizedAPIClient() *client.Client {
	once.Do(func() {
		client, err := client.NewClientWithConfig(client.LoadConfigFromEnv())
		if err != nil {
			panic(err)
		}
		centralizedapi = client
	})
	return centralizedapi
}
