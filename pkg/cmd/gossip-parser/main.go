// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

// gossip-parser is a simple utility program meant to parse the values from
// CockroachDB's gossip debug page (/_status/gossip/<node-id>) and print them
// in a human-readable format.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
)

var inputFile = flag.String("f", "", "file containing the JSON output from a node's _status/gossip/ endpoint")

func main() {
	ctx := context.Background()
	flag.Parse()

	file, err := os.Open(*inputFile)
	if err != nil {
		log.Fatalf(ctx, "failed to open provided file %q: %s", *inputFile, err)
	}
	defer file.Close()

	var gossipInfo gossip.InfoStatus
	if err := jsonpb.Unmarshal(file, &gossipInfo); err != nil {
		log.Fatalf(ctx, "failed to parse provided file as gossip.InfoStatus: %s", err)
	}

	var output []string
	for key, info := range gossipInfo.Infos {
		bytes, err := info.Value.GetBytes()
		if err != nil {
			log.Fatal(ctx, err)
		}
		if key == gossip.KeyClusterID || key == gossip.KeySentinel {
			clusterID, err := uuid.FromBytes(bytes)
			if err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %v", key, clusterID))
		} else if key == gossip.KeySystemConfig {
			// Uncomment this logic if you actually want to print the system config,
			// but it's big enough and prints poorly enough that it's not worth it
			// most of the time.
			/*
				var config config.SystemConfig
				if err := proto.Unmarshal(bytes, &config); err != nil {
					log.Fatal(ctx, err)
				}
				output = append(output, fmt.Sprintf("%q: %+v", key, config))
			*/
			output = append(output, fmt.Sprintf("%q: omitted", key))
		} else if key == gossip.KeyFirstRangeDescriptor {
			var desc roachpb.RangeDescriptor
			if err := proto.Unmarshal(bytes, &desc); err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %v", key, desc))
		} else if gossip.IsNodeIDKey(key) {
			var desc roachpb.NodeDescriptor
			if err := proto.Unmarshal(bytes, &desc); err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, desc))
		} else if strings.HasPrefix(key, gossip.KeyStorePrefix) {
			var desc roachpb.StoreDescriptor
			if err := proto.Unmarshal(bytes, &desc); err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, desc))
		} else if strings.HasPrefix(key, gossip.KeyNodeLivenessPrefix) {
			var liveness storage.Liveness
			if err := proto.Unmarshal(bytes, &liveness); err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, liveness))
		} else if strings.HasPrefix(key, gossip.KeyDeadReplicasPrefix) {
			var deadReplicas roachpb.StoreDeadReplicas
			if err := proto.Unmarshal(bytes, &deadReplicas); err != nil {
				log.Fatal(ctx, err)
			}
			output = append(output, fmt.Sprintf("%q: %+v", key, deadReplicas))
		}
	}

	sort.Strings(output)
	fmt.Println(strings.Join(output, "\n"))
}
