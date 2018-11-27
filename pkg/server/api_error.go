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

package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cockroachdb/cockroach/pkg/util/log"
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
