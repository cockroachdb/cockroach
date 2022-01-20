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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

// This file provides an implementation of the cancel cancellation
// APIs using a HMAC-SHA256-based exchange with variable nonces. It provides
// protection against replays, even when the cancel request is sent
// without TLS.

// HMACCancelProtocol is a protocol using HMACs and nonces
const HMACCancelProtocol CancelRequestProtocol = "h1"

// The authenticator type.
type hmacCancelAuth struct {
	sessionID    string
	sharedSecret string
	nonce        uint32
}

// The cancel client type.
type hmacCancelRequestClient struct {
	sessionID string
	clientKey string
	nonce     uint32
}

// How much bytes of entropy to use for shared keys.
const hmacSharedSecretLength = 16

func newHMACCancelRequestAuthenticator() CancelRequestAuthenticator {
	return &hmacCancelAuth{}
}

// Initialize is part of the CancelRequestAuthenticator interface.
func (s *hmacCancelAuth) Initialize(sessionID string) error {
	s.sessionID = sessionID
	s.nonce = 1
	sharedSecret := make([]byte, hmacSharedSecretLength)
	if _, err := io.ReadFull(rand.Reader, sharedSecret); err != nil {
		return errors.Wrap(err, "generating session secret")
	}
	s.sharedSecret = string(HMACCancelProtocol) + ":" + hex.EncodeToString(sharedSecret)
	return nil
}

// GetClientKey is part of the CancelRequestAuthenticator interface.
func (s *hmacCancelAuth) GetClientKey() CancelClientKey {
	key := simpleCancelS(s.sharedSecret)
	return &key
}

type hmacCancelRequest struct {
	nonce uint32
	hmac  []byte
}

func (h *hmacCancelRequest) String() string {
	return fmt.Sprintf("%s:%d:%x", HMACCancelProtocol, h.nonce, h.hmac)
}

var _ CancelRequest = (*hmacCancelRequest)(nil)
var hmacRequestRe = regexp.MustCompile(`^` + string(HMACCancelProtocol) + `:(\d+):([0-9a-f]+)$`)

// ParseCancelRequest is part of the CancelRequestAuthenticator interface.
func (s *hmacCancelAuth) ParseCancelRequest(r string) (CancelRequest, error) {
	parts := hmacRequestRe.FindStringSubmatch(r)
	if parts == nil {
		return nil, errors.Newf("invalid cancel request format: %q", r)
	}
	nonce, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return nil, errors.Wrap(err, "invalid nonce")
	}
	h, err := hex.DecodeString(parts[2])
	if err != nil {
		return nil, errors.Wrap(err, "invalid hmac")
	}
	return &hmacCancelRequest{nonce: uint32(nonce), hmac: h}, nil
}

// Authenticate is part of the CancelRequestAuthenticator interface.
func (s *hmacCancelAuth) Authenticate(
	ctx context.Context, sessionID string, request CancelRequest,
) (bool, bool, ClientMessage, error) {
	curNonce := s.nonce
	s.nonce++
	newNonce := hmacNonce(s.nonce)

	k, ok := request.(*hmacCancelRequest)
	if !ok {
		return false, false, &newNonce, errors.Newf("unknown request type: %T", request)
	}
	if sessionID != s.sessionID {
		return false, false, &newNonce, nil
	}
	success := false
	validNonce := curNonce == k.nonce
	if validNonce {
		// Compute the hmac.
		expH := computeCancelHMAC(s.sharedSecret, curNonce)
		if subtle.ConstantTimeCompare(expH, k.hmac) == 1 {
			success = true
		}
	}
	return success, !validNonce, &newNonce, nil
}

func computeCancelHMAC(sharedSecret string, nonce uint32) []byte {
	mac := hmac.New(sha256.New, []byte(sharedSecret))
	fmt.Fprintf(mac, "%d", nonce)
	return mac.Sum(nil)
}

func newHMACCancelRequestClient(sessionID string, clientKey string) (CancelRequestClient, error) {
	return &hmacCancelRequestClient{sessionID: sessionID, clientKey: clientKey, nonce: 1}, nil
}

// MakeRequest is part of the CancelRequestClient interface.
func (s *hmacCancelRequestClient) MakeRequest() (string, CancelRequest) {
	h := computeCancelHMAC(s.clientKey, s.nonce)
	req := &hmacCancelRequest{nonce: s.nonce, hmac: h}
	s.nonce++
	return s.sessionID, req
}

type hmacNonce uint32

var _ ClientMessage = (*hmacNonce)(nil)

func (n *hmacNonce) String() string { return fmt.Sprintf("%s:%d", HMACCancelProtocol, *n) }

// ParseClientMessage is part of the CancelRequestClient interface.
func (s *hmacCancelRequestClient) ParseClientMessage(msg string) (ClientMessage, error) {
	if !strings.HasPrefix(msg, string(HMACCancelProtocol)+":") {
		return nil, errors.Newf("malformed client message: %q", msg)
	}
	msg = msg[len(HMACCancelProtocol)+1:]
	nonce64, err := strconv.ParseUint(msg, 10, 32)
	nonce := uint32(nonce64)
	return (*hmacNonce)(&nonce), err
}

// UpdateFromServer is part of the CancelRequestClient interface.
func (s *hmacCancelRequestClient) UpdateFromServer(newNonce ClientMessage) error {
	n, ok := newNonce.(*hmacNonce)
	if !ok {
		return errors.Newf("unexpected message type: %T", newNonce)
	}
	if *n != 0 {
		s.nonce = uint32(*n)
	}
	return nil
}
