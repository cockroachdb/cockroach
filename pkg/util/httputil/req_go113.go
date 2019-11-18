// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build go1.13

package httputil

import (
	"context"
	"io"
	"net/http"
)

// NewRequestWithContext aliases http.NewRequestWithContext.
// TODO(knz): this can be removed when the repo is upgraded to use go 1.13.
func NewRequestWithContext(
	ctx context.Context, method, url string, body io.Reader,
) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, method, url, body)
}
