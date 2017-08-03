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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

var errAPIInternalError = grpc.Errorf(
	codes.Internal,
	"An internal server error has occurred. Please check your CockroachDB logs for more details.",
)

// apiError logs the provided error and returns an error that should be returned
// by the RPC endpoint method. This should be used to prevent internal server
// errors from returning messages to the browser.
func apiError(ctx context.Context, err error) error {
	log.ErrorfDepth(ctx, 1, "%s", err)
	return errAPIInternalError
}
