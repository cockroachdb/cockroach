// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAPIInternalErrorString = "An internal server error has occurred. Please check your CockroachDB logs for more details."

var errAPIInternalError = status.Errorf(
	codes.Internal,
	errAPIInternalErrorString,
)

// apiInternalError should be used to wrap server-side errors during API
// requests. This method records the contents of the error to the server log,
// and returns a standard GRPC error which is appropriate to return to the
// client.
func apiInternalError(ctx context.Context, err error) error {
	log.ErrorfDepth(ctx, 1, "%s", err)
	return errAPIInternalError
}

// apiV2InternalError should be used to wrap server-side errors during API
// requests for V2 (non-GRPC) endpoints. This method records the contents
// of the error to the server log, and sends the standard internal error string
// over the http.ResponseWriter.
func apiV2InternalError(ctx context.Context, err error, w http.ResponseWriter) {
	log.ErrorfDepth(ctx, 1, "%s", err)
	http.Error(w, errAPIInternalErrorString, http.StatusInternalServerError)
}
