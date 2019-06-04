// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

// +build !windows

package sdnotify

import (
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
)

func TestSDNotify(t *testing.T) {
	l, err := listen()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.close() }()

	ch := make(chan error)
	go func() {
		ch <- l.wait()
	}()

	if err := notify(l.Path, readyMsg); err != nil {
		t.Fatal(err)
	}
	if err := <-ch; err != nil {
		t.Fatal(err)
	}
}
