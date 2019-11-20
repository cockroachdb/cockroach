// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !go1.13

package httputil

import (
	"context"
	"io"
	"net/http"
)

// NewRequestWithContext provides the same interface as the Go 1.13 function
// in http but ignores the context argument.
//
// This is transition code until the repository is upgraded to use Go
// 1.13. In the meantime, the callers rely on requests eventually
// succeeding or failing with a timeout if the remote server does not
// provide a response on time.
//
// TODO(knz): remove this when the repo does not use 1.12 any more.
func NewRequestWithContext(
	ctx context.Context, method, url string, body io.Reader,
) (*http.Request, error) {
	return http.NewRequest(method, url, body)
}
