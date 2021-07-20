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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	ctx context.Context, req *serverpb.CARequest,
) (*serverpb.CAResponse, error) {
	settings := s.server.ClusterSettings()
	if settings == nil {
		return nil, errors.AssertionFailedf("could not look up cluster settings")
	}
	if !sql.FeatureTLSAutoJoinEnabled.Get(&settings.SV) {
		return nil, errors.New("feature disabled by administrator")
	}

	cm, err := s.server.rpcContext.GetCertificateManager()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get certificate manager")
	}
	caCert := cm.CACert().FileContents

	res := &serverpb.CAResponse{
		CaCert: caCert,
	}
	return res, nil
}

func (s *adminServer) consumeJoinToken(ctx context.Context, clientToken security.JoinToken) error {
	return s.server.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		row, err := s.ie.QueryRow(
			ctx, "select-consume-join-token", txn,
			"SELECT id, secret FROM system.join_tokens WHERE id = $1 AND now() < expiration",
			clientToken.TokenID.String())
		if err != nil {
			return err
		} else if len(row) != 2 {
			return ErrInvalidAddJoinToken
		}

		secret := *row[1].(*tree.DBytes)
		if !bytes.Equal([]byte(secret), clientToken.SharedSecret) {
			return errors.New("invalid shared secret")
		}

		i, err := s.ie.Exec(ctx, "delete-consume-join-token", txn,
			"DELETE FROM system.join_tokens WHERE id = $1",
			clientToken.TokenID.String())
		if err != nil {
			return err
		} else if i == 0 {
			return errors.New("error when consuming join token: no token found")
		}

		return nil
	})
}

// RequestCertBundle makes it possible for a node to request its TLS certs from
// another node. It will validate and attempt to consume a token with the uuid
// and shared secret provided.
func (s *adminServer) RequestCertBundle(
	ctx context.Context, req *serverpb.CertBundleRequest,
) (*serverpb.CertBundleResponse, error) {
	settings := s.server.ClusterSettings()
	if settings == nil {
		return nil, errors.AssertionFailedf("could not look up cluster settings")
	}
	if !sql.FeatureTLSAutoJoinEnabled.Get(&settings.SV) {
		return nil, errors.New("feature disabled by administrator")
	}

	var err error
	var clientToken security.JoinToken
	clientToken.SharedSecret = req.SharedSecret
	clientToken.TokenID, err = uuid.FromString(req.TokenID)
	if err != nil {
		return nil, ErrInvalidAddJoinToken
	}

	// Attempt to consume clientToken, error if unsuccessful.
	if err := s.consumeJoinToken(ctx, clientToken); err != nil {
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

	res := &serverpb.CertBundleResponse{
		Bundle: bundleBytes,
	}
	return res, nil
}
