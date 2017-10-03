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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/kr/pretty"
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

// makeKeyedVersions validates the input and initializes a keyedVersions. The given slice must be
// sorted by descending version (i.e. new versions go first).
func makeKeyedVersions(vs []keyedVersion) keyedVersions {
	ctx := context.Background()
	// Reverse the slice.
	for i, n := 0, len(vs); i < n-i; i++ {
		vs[i], vs[n-i-1] = vs[n-i-1], vs[i]
	}
	kv := keyedVersions(vs)
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
