// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

// This file intentionally does not require the "acceptance" build tag in order
// to silence a warning from the emacs flycheck package.

package acceptance

import (
	"testing"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
)

// makeDBClient creates a DB client for node 'i' using the cluster certs dir.
func makeDBClient(t *testing.T, cluster *localcluster.Cluster, node int) *client.DB {
	return makeDBClientForUser(t, cluster, "root", node)
}

// makeDBClientForUser creates a DB client for node 'i' and user 'user'.
func makeDBClientForUser(t *testing.T, cluster *localcluster.Cluster, user string, node int) *client.DB {
	// We need to run with "InsecureSkipVerify" (set when Certs="" inside the http sender).
	// This is due to the fact that we're running outside docker, so we cannot use a fixed hostname
	// to reach the cluster. This in turn means that we do not have a verified server name in the certs.
	c, err := client.Open("https://" + user + "@" +
		cluster.Nodes[node].Addr("").String() +
		"?certs=")

	if err != nil {
		t.Fatal(err)
	}
	return c
}

// setDefaultRangeMaxBytes sets the range-max-bytes value for the default zone.
func setDefaultRangeMaxBytes(t *testing.T, c *client.DB, maxBytes int64) {
	zone := &proto.ZoneConfig{}
	if err := c.GetProto(keys.ConfigZonePrefix, zone); err != nil {
		t.Fatal(err)
	}
	if zone.RangeMaxBytes == maxBytes {
		return
	}
	zone.RangeMaxBytes = maxBytes
	if err := c.Put(keys.ConfigZonePrefix, zone); err != nil {
		t.Fatal(err)
	}
}

// getPermConfig fetches the permissions config for 'prefix'.
func getPermConfig(client *client.DB, prefix string) (*proto.PermConfig, error) {
	config := &proto.PermConfig{}
	if err := client.GetProto(keys.MakeKey(keys.ConfigPermissionPrefix, proto.Key(prefix)), config); err != nil {
		return nil, err
	}

	return config, nil
}

// putPermConfig writes the permissions config for 'prefix'.
func putPermConfig(client *client.DB, prefix string, config *proto.PermConfig) error {
	return client.Put(keys.MakeKey(keys.ConfigPermissionPrefix, proto.Key(prefix)), config)
}
