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

package cluster

import (
	"context"

	"github.com/kr/pretty"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// keyedVersion associates a key to a version.
type keyedVersion struct {
	Key VersionKey
	roachpb.Version
}

// keyedVersions is a container for managing the versions of CockroachDB.
type keyedVersions []keyedVersion

// MustByKey asserts that the version specified by this key exists, and returns it.
func (kv keyedVersions) MustByKey(k VersionKey) roachpb.Version {
	key := int(k)
	if key >= len(kv) || key < 0 {
		log.Fatalf(context.Background(), "version with key %d does not exist, have:\n%s",
			key, pretty.Sprint(kv))
	}
	return kv[key].Version
}

// Validated returns the receiver after asserting that versions are in chronological order
// and that their keys correspond to their position in the list.
func (kv keyedVersions) Validated() keyedVersions {
	ctx := context.Background()
	for i, namedVersion := range kv {
		if int(namedVersion.Key) != i {
			log.Fatalf(ctx, "version %s should have key %d but has %d",
				namedVersion, i, namedVersion.Key)
		}
		if i > 0 {
			prev := kv[i-1]
			if !prev.Version.Less(namedVersion.Version) {
				log.Fatalf(ctx, "version %s must be larger than %s", namedVersion, prev)
			}
		}
	}
	return kv
}
