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
	"crypto/rand"
	"encoding/hex"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
)

// This file provides an implementation of the cancel cancellation
// APIs using a simple shared secret. The cancel requests are
// replayable.

// SimpleCancelProtocol is a protocol using a simple shared secret.
const SimpleCancelProtocol CancelRequestProtocol = "s"

// The authenticator type.
type simpleCancelAuth struct {
	sessionID    string
	sharedSecret string
}

// The cancel client type.
type simpleCancelRequestClient struct {
	sessionID string
	clientKey string
}

// How much bytes of entropy to use for shared keys.
const simpleSharedSecretLength = 16

func newSimpleCancelRequestAuthenticator() CancelRequestAuthenticator {
	return &simpleCancelAuth{}
}

// Initialize is part of the CancelRequestAuthenticator interface.
func (s *simpleCancelAuth) Initialize(sessionID string) error {
	s.sessionID = sessionID
	sharedSecret := make([]byte, simpleSharedSecretLength)
	if _, err := io.ReadFull(rand.Reader, sharedSecret); err != nil {
		return errors.Wrap(err, "generating session secret")
	}
	s.sharedSecret = string(SimpleCancelProtocol) + ":" + hex.EncodeToString(sharedSecret)
	return nil
}

// GetClientKey is part of the CancelRequestAuthenticator interface.
func (s *simpleCancelAuth) GetClientKey() CancelClientKey {
	key := simpleCancelS(s.sharedSecret)
	return &key
}

type simpleCancelS string

func (s simpleCancelS) String() string { return string(s) }

var _ CancelClientKey = (*simpleCancelS)(nil)
var _ CancelRequest = (*simpleCancelS)(nil)

// ParseCancelRequest is part of the CancelRequestAuthenticator interface.
func (s *simpleCancelAuth) ParseCancelRequest(r string) (CancelRequest, error) {
	return parseSimpleRequest(r)
}

func parseSimpleRequest(r string) (CancelRequest, error) {
	if !strings.HasPrefix(r, string(SimpleCancelProtocol)+":") || len(r) != len(SimpleCancelProtocol)+1+2* /*hex*/ simpleSharedSecretLength {
		return nil, errors.Newf("invalid cancel request format: %q", r)
	}
	return (*simpleCancelS)(&r), nil
}

// Authenticate is part of the CancelRequestAuthenticator interface.
func (s *simpleCancelAuth) Authenticate(
	ctx context.Context, sessionID string, request CancelRequest,
) (bool, bool, ClientMessage, error) {
	k, ok := request.(*simpleCancelS)
	if !ok {
		return false, false, nil, errors.Newf("unknown request type: %T", request)
	}
	return sessionID == s.sessionID && string(*k) == s.sharedSecret, false, nilMessage{}, nil
}

type nilMessage struct{}

var _ ClientMessage = nilMessage{}

func (s nilMessage) String() string { return "" }

func newSimpleCancelRequestClient(sessionID string, clientKey string) (CancelRequestClient, error) {
	// In the simple protocol, client key == request.
	_, err := parseSimpleRequest(clientKey)
	if err != nil {
		return nil, err
	}
	return &simpleCancelRequestClient{sessionID: sessionID, clientKey: clientKey}, nil
}

// MakeRequest is part of the CancelRequestClient interface.
func (s *simpleCancelRequestClient) MakeRequest() (string, CancelRequest) {
	// In the simple protocol, client key == request.
	return s.sessionID, (*simpleCancelS)(&s.clientKey)
}

// ParseClientMessage is part of the CancelRequestClient interface.
func (s *simpleCancelRequestClient) ParseClientMessage(_ string) (ClientMessage, error) {
	// There's no client message in the simple protocol.
	return nil, nil
}

// UpdateFromServer is part of the CancelRequestClient interface.
func (s *simpleCancelRequestClient) UpdateFromServer(_ ClientMessage) error {
	// Unused in this protocol.
	return nil
}
