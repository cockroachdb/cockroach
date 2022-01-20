// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
)

// DoCancelRequest processes a one-short cancel request from a SQL client,
// presumably a SQL shell.
//
// This is the function called by pgwire upon reception of a
// "combined" cancel request from a client over the network. A
// combined cancel request is the concatenation of the target session
// ID and a cancel request, separated by ":".
func (s *Server) DoCancelRequest(
	ctx context.Context, combinedCancelRequest string,
) (*serverpb.CancelQueryByKeyResponse, error) {
	// Limit the number of concurrent cancel requests, so as to prevent
	// attackers from flooding a cluster with cross-node RPCs.
	sem := getCancelRequestSem(ctx)
	alloc, err := sem.Acquire(ctx, 1)
	if err != nil {
		return nil, err
	}
	defer alloc.Release()

	idx := strings.IndexByte(combinedCancelRequest, ':')
	if idx == -1 {
		return nil, pgerror.Newf(pgcode.Syntax, "malformed request: %q", combinedCancelRequest)
	}
	sessionIDs := combinedCancelRequest[:idx]
	cancelRequest := combinedCancelRequest[idx+1:]

	request := &serverpb.CancelQueryByKeyRequest{
		SessionID:     sessionIDs,
		CancelRequest: cancelRequest,
	}
	return s.cfg.SQLStatusServer.CancelQueryByKey(ctx, request)
}

// cancelCurrentQuery is part of the registrySession interface.
// This is the function called on the node that's responsible
// for the session whose current query(ies) is being cancelled.
// This is also where the authentication takes place.
func (ex *connExecutor) cancelQueryByKey(
	ctx context.Context, sessionID string, cancelRequest string,
) (bool, bool, string, error) {
	ex.mu.Lock()
	defer ex.mu.Unlock()

	// Verify that the request is valid.
	auth := ex.mu.CancelRequestAuthenticator
	req, err := auth.ParseCancelRequest(cancelRequest)
	if err != nil {
		return false, false, "", err
	}
	ok, retry, msg, err := auth.Authenticate(ctx, sessionID, req)
	msgS := ""
	if msg != nil {
		msgS = msg.String()
	}
	if !ok || retry || err != nil {
		if !ok && !retry {
			err = errors.CombineErrors(err, errors.New("cannot authenticate request"))
		}
		return ok, retry, msgS, err
	}

	// The request is valid. Do the canellation.
	hasQueries := len(ex.mu.ActiveQueries) > 0
	for _, queryMeta := range ex.mu.ActiveQueries {
		queryMeta.cancel()
	}
	return hasQueries, retry, msgS, nil
}

var cancelRequestSemOnce struct {
	sem  *quotapool.IntPool
	once sync.Once
}

// getCancelRequestSem retrieves the hashing semaphore.
func getCancelRequestSem(ctx context.Context) *quotapool.IntPool {
	cancelRequestSemOnce.once.Do(func() {
		cancelRequestSemOnce.sem = quotapool.NewIntPool("cancel_requests", 1)
	})
	return cancelRequestSemOnce.sem
}
