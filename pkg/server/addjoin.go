// Copyright 2021 The Cockroach Authors.
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
	"bytes"
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const dummyJoinTokenID = "c72fbec2-2bc2-4491-8b21-a847dc3b7746"
const dummyJoinTokenSharedSecret = "sEcr3t!"

// ErrInvalidAddJoinToken is an error to signal server rejected Add/Join token as invalid.
var ErrInvalidAddJoinToken = errors.New("invalid add/join token received")

// ErrAddJoinTokenConsumed is an error to signal the server consumed the Add/Join token but failed
// to provide the CertBundle to the client.
var ErrAddJoinTokenConsumed = errors.New("add/join token consumed but then another error occurred")

// RequestCA makes it possible for a node to request the node-to-node CA certificate.
func (s *adminServer) RequestCA(
	ctx context.Context, req *serverpb.CaRequest,
) (*serverpb.CaResponse, error) {
	cl := security.MakeCertsLocator(s.server.cfg.SSLCertsDir)
	caCert, err := loadCertificateFile(cl.CACertPath())
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to read inter-node cert from disk at %q ",
			cl.CACertPath(),
		)
	}

	res := &serverpb.CaResponse{
		CaCert: caCert,
	}
	return res, nil
}

// GenerateJoinToken creates a joinToken that includes the fingerprint of
// caCert signed with a randomly generated sharedSecret.
// TODO(aaron-crl): Implement this.
func GenerateJoinToken(caCert []byte) ([]byte, error) {
	var j joinToken
	var err error
	j.tokenID, err = uuid.FromString(dummyJoinTokenID)
	if err != nil {
		return nil, err
	}
	j.sharedSecret = []byte(dummyJoinTokenSharedSecret)
	j.sign(caCert)
	return j.MarshalText()
}

// TODO(aarcon-crl): Implement this.
func (j joinToken) consumeJoinToken() error {
	dummyUUID, _ := uuid.FromString(dummyJoinTokenID)
	if j.tokenID != dummyUUID {
		return ErrInvalidAddJoinToken
	}
	if bytes.Equal(j.sharedSecret, []byte(dummyJoinTokenSharedSecret)) {
		return ErrInvalidAddJoinToken
	}
	return nil
}

// RequestCertBundle makes it possible for a node to request its TLS certs from
// another node. It will validate and attempt to consume a token with the uuid
// and shared secret provided.
func (s *adminServer) RequestCertBundle(
	ctx context.Context, req *serverpb.BundleRequest,
) (*serverpb.BundleResponse, error) {
	var err error
	var clientToken joinToken
	clientToken.sharedSecret = []byte(req.SharedSecret)
	clientToken.tokenID, err = uuid.FromString(req.TokenID)
	if err != nil {
		return nil, ErrInvalidAddJoinToken
	}

	// Attempt to consume clientToken, error if unsuccessful.
	if err := clientToken.consumeJoinToken(); err != nil {
		return nil, err
	}

	// Collect certs to send to client.
	certBundle, err := collectLocalCABundle(s.server.cfg.SSLCertsDir)
	if err != nil {
		// TODO(aaron-crl): Log the reason for the error on the server.
		// errors.Wrapf(err, "failed to collect LocalCABundle")

		return nil, ErrAddJoinTokenConsumed
	}

	bundleBytes, err := json.Marshal(certBundle)
	if err != nil {
		// TODO(aaron-crl): Log the reason for the error on the server.
		//errors.Wrapf(err, "failed to marshal LocalCABundle")

		return nil, ErrAddJoinTokenConsumed
	}

	res := &serverpb.BundleResponse{
		Bundle: bundleBytes,
	}
	return res, nil
}
