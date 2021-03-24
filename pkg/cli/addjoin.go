// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"

	"github.com/cockroachdb/errors"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var nodeJoinCmd = &cobra.Command{
	Use:   "node-join <remote-addr>",
	Short: "request the TLS certs for a new node from an existing node",
	Args:  cobra.MinimumNArgs(1),
	RunE:  MaybeDecorateGRPCError(runNodeJoin),
}

func requestPeerCA(ctx context.Context, peer string, joinToken string) (*x509.CertPool, error) {
	var dialOpts []grpc.DialOption

	dialOpts = rpc.GetAddJoinDialOptions(nil)

	conn, err := grpc.DialContext(ctx, peer, dialOpts...)
	if err != nil {
		return nil, err
	}

	s := serverpb.NewAdminClient(conn)

	req := serverpb.CaRequest{}
	resp, err := s.RequestCA(ctx, &req)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed grpc call to request CA from peer")
	}

	// Verify that the received bytes match our expected MAC.
	isTrustedCA, err := server.IsCATrustedByJoinToken(resp.CaCert, joinToken)
	if err != nil {
		return nil, errors.Wrap(err, "failed to validate remote CA")
	}
	if !isTrustedCA {
		return nil, errors.New("resp.CaCert failed cryptologic validation")
	}

	// Parse them to an x509.Certificate then add them to a pool.
	pemBlock, _ := pem.Decode(resp.CaCert)
	if pemBlock == nil {
		return nil, errors.New("failed to parse valid PEM from resp.CaCert")
	}
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return nil, errors.New("failed to parse valid x509 cert from resp.CaCert")
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(cert)

	return certPool, nil
}

// runNodeJoin will attempt to connect to peers from the list provided and
// request a certificate initialization bundle if it is able to validate a
// peer.
// TODO(aaron-crl): Parallelize this and handle errors.
func runNodeJoin(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// TODO(aaron-crl) plumb this into the connect or start commands or both.
	var err error
	peerAddr := args[0]
	jt := args[1]

	// TODO(aaron-crl): loop over peers here.
	// For each peer, attempt to validate it's CA, then request a bundle with
	// the supplied join-token. For now loop this to reduce complexity.
	certPool, err := requestPeerCA(ctx, peerAddr, jt)
	if err != nil {
		return errors.Wrapf(
			err, "failed requesting peer CA from %q", peerAddr)
	}

	// TODO(aaron-crl): Update add/join to signal to client when a token IS
	// consumed.
	certBundle, err := requestCertBundle(ctx, peerAddr, certPool)
	if err != nil {
		return errors.Wrapf(
			err, "failed requesting certBundle from peer %q, token may have been consumed", peerAddr)
	}

	// Use the bundle to initialize the node.
	err = certBundle.InitializeNodeFromBundle(ctx, *baseCfg)
	if err != nil {
		return errors.Wrap(
			err,
			"failed to initialize node after consuming join-token",
		)
	}

	return nil
}

func requestCertBundle(
	ctx context.Context, peerAddr string, certPool *x509.CertPool,
) (*server.CertificateBundle, error) {
	var dialOpts []grpc.DialOption
	dialOpts = rpc.GetAddJoinDialOptions(certPool)

	conn, err := grpc.DialContext(ctx, peerAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	s := serverpb.NewAdminClient(conn)
	req := serverpb.BundleRequest{}
	resp, err := s.RequestCertBundle(ctx, &req)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to RequestCertBundle from %q",
			peerAddr,
		)
	}

	var certBundle server.CertificateBundle
	err = json.Unmarshal(resp.Bundle, &certBundle)
	if err != nil {
		return nil, errors.Wrapf(
			err,
			"failed to unmarshal CertBundle from %q",
			peerAddr,
		)
	}

	return &certBundle, nil
}
