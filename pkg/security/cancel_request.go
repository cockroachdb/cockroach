// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package security

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
)

// This file contains the API definition to create and authenticate
// SQL cancellation requests.

// CancelRequestAuthenticator is the object stored per-session on the
// server, which is able to authenticate incoming query cancellation
// requests.
type CancelRequestAuthenticator interface {
	// Initialize resets and prepares the authenticator.
	// sessionID is the session for which this authenticator
	// is validating cancel requests.
	// (We are not using the sql.ClusterWideID type in order
	// to avoid a dependency cycle.)
	Initialize(sessionID string) error

	// GetClientKey retrieves the client key to provide
	// to the client when it initially opens the session.
	GetClientKey() CancelClientKey

	// ParseCancelRequest turns a string converted from
	// CancelRequest.String() back into a CancelRequest.
	ParseCancelRequest(string) (CancelRequest, error)

	// Authenticate is the method to call when a cancel request is
	// received by a server, to determine whether the cancel request
	// is valid. The sessionID argument is the target session requested
	// by the client.
	Authenticate(ctx context.Context, sessionID string, request CancelRequest) (ok, shouldRetry bool, update ClientMessage, err error)
}

// CancelClientKey is the type of a cancel client key.
type CancelClientKey interface {
	fmt.Stringer
}

// CancelRequest is the type of a cancel request.
type CancelRequest interface {
	fmt.Stringer
}

// ClientMessage is the type of the payload sent
// back from the authenticator running on the server,
// to the CancelRequestClient running on the client.
// It should be provided to the CancelRequestClient via
// the UpdateFromServer() method.
// CancelRequest is the type of a cancel request.
type ClientMessage interface {
	fmt.Stringer
}

// CancelRequestClient is the object that should be instantiated
// by SQL clients that wish to issue cancel requests to a server.
type CancelRequestClient interface {
	// MakeRequest creates a new cancel request for the current session.
	MakeRequest() (sessionID string, req CancelRequest)

	// Parse converts the result of ClientMessage.String() on the server
	// back into a ClientMessage on the client.
	ParseClientMessage(s string) (ClientMessage, error)

	// UpdateFromServer should be called by the client when
	// receiving a client message in response to an Authenticate
	// call server-side.
	UpdateFromServer(srvMsg ClientMessage) error
}

// NewCancelRequestAuthenticator returns a CancelRequestAuthenticator
// for the given protocol.
func NewCancelRequestAuthenticator(
	proto CancelRequestProtocol,
) (CancelRequestAuthenticator, error) {
	switch proto {
	case SimpleCancelProtocol:
		return newSimpleCancelRequestAuthenticator(), nil
	case HMACCancelProtocol:
		return newHMACCancelRequestAuthenticator(), nil
	default:
		return nil, errors.Newf("unrecognized cancel key protocol: %q", proto)
	}
}

// NewCancelRequestClient returns a CanceRequestClient
// suitable for minting cancel requests given a client
// key representation provided by a server.
// sessionID is the session for which this client operates.
// It should match the sessionID provided to the server-side
// authenticator.
// clientKey is the client key received from the server
// when the session was opened. It should be the
// same as returned by GetClientKey() on the authenticator.
func NewCancelRequestClient(sessionID string, cancelKeyRepr string) (CancelRequestClient, error) {
	idx := strings.IndexByte(cancelKeyRepr, ':')
	if idx == -1 {
		return nil, errors.Newf("malformed cancel key: %v", cancelKeyRepr)
	}
	proto := CancelRequestProtocol(cancelKeyRepr[:idx])
	switch proto {
	case SimpleCancelProtocol:
		return newSimpleCancelRequestClient(sessionID, cancelKeyRepr)
	case HMACCancelProtocol:
		return newHMACCancelRequestClient(sessionID, cancelKeyRepr)
	default:
		return nil, errors.Newf("unrecognized cancel key protocol: %q", proto)
	}
}

// CancelRequestProtocol is the type of protocol to use
// to validate cancel requests. Must match between server and client.
type CancelRequestProtocol string
