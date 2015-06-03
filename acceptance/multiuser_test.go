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
// Author: Marc Berhault (marc@cockroachlabs.com)

// +build acceptance

package acceptance

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/acceptance/localcluster"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
)

// TestMultiuser starts up an N node cluster and performs various ops
// using different users.
func TestMultiuser(t *testing.T) {
	l := localcluster.Create(*numNodes, stopper)
	l.Start()
	defer l.Stop()

	checkRangeReplication(t, l, 20*time.Second)

	// Make clients.
	rootClient := makeDBClientForUser(t, l, "root", 0)
	fooClient := makeDBClientForUser(t, l, "foo", 0)
	otherClient := makeDBClientForUser(t, l, "other", 0)

	// Set permissions configs.
	configs := []struct {
		prefix  string
		readers []string
		writers []string
	}{
		// Good to know: "root" is always allowed to read and write.
		{"foo", []string{"foo"}, []string{"foo"}},
		{"foo/public", []string{"foo", "other"}, []string{"foo"}},
		{"tmp", []string{"foo", "other"}, []string{"foo", "other"}},
	}
	for i, cfg := range configs {
		protoConfig := &proto.PermConfig{Read: cfg.readers, Write: cfg.writers}
		if err := putPermConfig(rootClient, cfg.prefix, protoConfig); err != nil {
			t.Fatalf("#%d: failed to write config %+v for prefix %q: %v", i, protoConfig, cfg.prefix, err)
		}
	}

	// Write some data. The value is just the key.
	writes := []struct {
		key     string
		client  *client.DB
		success bool
	}{
		{"some-file", rootClient, true}, {"some-file", fooClient, false}, {"some-file", otherClient, false},
		{"foo/a", rootClient, true}, {"foo/a", fooClient, true}, {"foo/a", otherClient, false},
		{"foo/public/b", rootClient, true}, {"foo/public/b", fooClient, true}, {"foo/public/b", otherClient, false},
		{"tmp/c", rootClient, true}, {"tmp/c", fooClient, true}, {"tmp/c", otherClient, true},
	}

	for i, w := range writes {
		_, err := w.client.Put(w.key, w.key)
		if (err == nil) != w.success {
			t.Errorf("test case #%d: %+v, got err=%v", i, w, err)
		}
	}

	// Read the previously-written files. They all succeeded at least once.
	reads := []struct {
		key     string
		client  *client.DB
		success bool
	}{
		{"some-file", rootClient, true}, {"some-file", fooClient, false}, {"some-file", otherClient, false},
		{"foo/a", rootClient, true}, {"foo/a", fooClient, true}, {"foo/a", otherClient, false},
		{"foo/public/b", rootClient, true}, {"foo/public/b", fooClient, true}, {"foo/public/b", otherClient, true},
		{"tmp/c", rootClient, true}, {"tmp/c", fooClient, true}, {"tmp/c", otherClient, true},
	}

	for i, r := range reads {
		_, err := r.client.Get(r.key)
		if (err == nil) != r.success {
			t.Errorf("test case #%d: %+v, got err=%v", i, r, err)
		}
	}
}
