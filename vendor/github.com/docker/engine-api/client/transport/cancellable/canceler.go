// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build go1.5

package cancellable

import (
	"net/http"

	"github.com/docker/engine-api/client/transport"
)

func canceler(client transport.Sender, req *http.Request) func() {
	// TODO(djd): Respect any existing value of req.Cancel.
	ch := make(chan struct{})
	req.Cancel = ch

	return func() {
		close(ch)
	}
}
