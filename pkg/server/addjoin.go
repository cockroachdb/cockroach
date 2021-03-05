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
	"context"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

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

// RequestCertBundle makes it possible for a node to request its TLS certs from another node.
func (s *adminServer) RequestCertBundle(
	ctx context.Context, req *serverpb.BundleRequest,
) (*serverpb.BundleResponse, error) {
	// Validate Add/Join token sharedSecret.
	var err error
	jt := joinToken{}
	jt.tokenID, err = uuid.FromString(req.TokenID)
	if err != nil {
		return nil, ErrInvalidAddJoinToken
	}
	isValidToken, err := jt.isValid(s.server.gossip)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed to validate token")
	}
	if !isValidToken {
		return nil, ErrInvalidAddJoinToken
	}

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
