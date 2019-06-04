// Copyright 2017 The Cockroach Authors.
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

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errAPIInternalError = status.Errorf(
	codes.Internal,
	"An internal server error has occurred. Please check your CockroachDB logs for more details.",
)

// apiInternalError should be used to wrap server-side errors during API
// requests. This method records the contents of the error to the server log,
// and returns a standard GRPC error which is appropriate to return to the
// client.
func apiInternalError(ctx context.Context, err error) error {
	log.ErrorfDepth(ctx, 1, "%s", err)
	return errAPIInternalError
}
