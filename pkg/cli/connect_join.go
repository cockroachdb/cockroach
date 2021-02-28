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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

const nodeJoinTimeout = 1 * time.Minute

var connectJoinCmd = &cobra.Command{
	Use:   "join <join-token>",
	Short: "request the TLS certs for a new node from an existing node",
	Args:  cobra.MinimumNArgs(1),
	RunE:  MaybeDecorateGRPCError(runConnectJoin),
}

func requestPeerCA(
	ctx context.Context, stopper *stop.Stopper, peer string, jt security.JoinToken,
) (*x509.CertPool, error) {
	dialOpts := rpc.GetAddJoinDialOptions(nil)

	conn, err := grpc.DialContext(ctx, peer, dialOpts...)
	if err != nil {
		return nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	s := serverpb.NewAdminClient(conn)

	req := serverpb.CARequest{}
	resp, err := s.RequestCA(ctx, &req)
	if err != nil {
		return nil, errors.Wrap(
			err, "failed grpc call to request CA from peer")
	}

	if !jt.VerifySignature(resp.CaCert) {
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

func requestCertBundle(
	ctx context.Context,
	stopper *stop.Stopper,
	peerAddr string,
	certPool *x509.CertPool,
	jt security.JoinToken,
) (*server.CertificateBundle, error) {
	dialOpts := rpc.GetAddJoinDialOptions(certPool)

	conn, err := grpc.DialContext(ctx, peerAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	s := serverpb.NewAdminClient(conn)
	req := serverpb.CertBundleRequest{
		TokenID:      jt.TokenID.String(),
		SharedSecret: jt.SharedSecret,
	}
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

// runConnectJoin will attempt to connect to peers from the join list provided and
// request a certificate initialization bundle if it is able to validate a
// peer.
// TODO(aaron-crl): Parallelize this and handle errors.
func runConnectJoin(cmd *cobra.Command, args []string) error {
	return contextutil.RunWithTimeout(context.Background(), "init handshake", nodeJoinTimeout, func(ctx context.Context) error {
		ctx = logtags.AddTag(ctx, "init-tls-handshake", nil)

		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)

		if err := validateNodeJoinFlags(cmd); err != nil {
			return err
		}

		joinTokenArg := args[0]
		var jt security.JoinToken
		if err := jt.UnmarshalText([]byte(joinTokenArg)); err != nil {
			return errors.Wrap(err, "failed to parse join token")
		}

		for _, peer := range serverCfg.JoinList {
			certPool, err := requestPeerCA(ctx, stopper, peer, jt)
			if err != nil {
				// Try a different peer.
				log.Errorf(ctx, "failed requesting peer CA from %s: %s", peer, err)
				continue
			}

			// TODO(aaron-crl): Update add/join to signal to client when a token IS
			// consumed.
			certBundle, err := requestCertBundle(ctx, stopper, peer, certPool, jt)
			if err != nil {
				return errors.Wrapf(err,
					"failed requesting certBundle from peer %q, token may have been consumed", peer)
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

		return errors.New("could not successfully authenticate with any listed nodes")
	})
}

func validateNodeJoinFlags(_ *cobra.Command) error {
	if len(serverCfg.JoinList) == 0 {
		return errors.Newf("flag --%s must specify address of at least one node to join",
			cliflags.Join.Name)
	}
	return nil
}
