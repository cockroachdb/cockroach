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
	"fmt"
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
	Short: "request the TLS certificates for a new node from an initialized cluster",
	Long: `
Connects to a CockroachDB node started with 'start' or 'start-single-node'
and obtain a package of TLS certificates for use with secure inter-node connections.

The TLS certificates are saved in the configured target directory.

This command requires a join token created by an administrator account
on the existing cluster using the SQL built-in function
crdb_internal.create_join_token().
`,
	Args: cobra.MinimumNArgs(1),
	RunE: MaybeDecorateGRPCError(runConnectJoin),
}

func requestPeerCA(
	ctx context.Context,
	stopper *stop.Stopper,
	report func(string, ...interface{}),
	peer string,
	jt security.JoinToken,
) (retryable bool, caCert *x509.CertPool, err error) {
	report("connecting to %q anonymously...", peer)
	dialOpts := rpc.GetAddJoinDialOptions(nil)
	conn, err := grpc.DialContext(ctx, peer, dialOpts...)
	if err != nil {
		return true, nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	s := serverpb.NewAdminClient(conn)

	report("requesting CA...")
	req := serverpb.CARequest{}
	resp, err := s.RequestCA(ctx, &req)
	if err != nil {
		return true, nil, err
	}

	// Everything after this point is non-retriable: they come from
	// either bugs server-side, a different version of CockroachDB,
	// or the user did not use a token minted for the right cluster.

	report("checking retrieved CA against join token...")
	if !jt.VerifySignature(resp.CaCert) {
		return false, nil, errors.WithHint(
			errors.New("CA certificate could not be verified with the join token"),
			"The join token is invalid or was generated for a different cluster.")
	}

	report("extracting CA certificate...")
	// Parse them to an x509.Certificate then add them to a pool.
	pemBlock, _ := pem.Decode(resp.CaCert)
	if pemBlock == nil {
		return false, nil, errors.AssertionFailedf("unexpected: retrieved CA certificate not in PEM format")
	}
	cert, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		return false, nil, errors.NewAssertionErrorWithWrappedErrf(err, "cannot parse retrieved CA certificate")
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(cert)

	return false, certPool, nil
}

const errJoinTokenConsumedHint = "The join token has been consumed by the error, you will need to re-generated a token before retrying."

func requestCertBundle(
	ctx context.Context,
	stopper *stop.Stopper,
	report func(string, ...interface{}),
	peerAddr string,
	certPool *x509.CertPool,
	jt security.JoinToken,
) (*server.CertificateBundle, error) {
	report("re-connecting to %q with CA signature...", peerAddr)
	dialOpts := rpc.GetAddJoinDialOptions(certPool)
	conn, err := grpc.DialContext(ctx, peerAddr, dialOpts...)
	if err != nil {
		return nil, err
	}
	stopper.AddCloser(stop.CloserFn(func() {
		_ = conn.Close() // nolint:grpcconnclose
	}))

	s := serverpb.NewAdminClient(conn)

	report("retrieving certificate bundle using join token...")
	req := serverpb.CertBundleRequest{
		TokenID:      jt.TokenID.String(),
		SharedSecret: jt.SharedSecret,
	}
	resp, err := s.RequestCertBundle(ctx, &req)
	if err != nil {
		return nil, errors.WithHint(
			errors.Wrap(err, "failed to request bundle"),
			"Token may have been consumed already.")
	}

	report("extracting bundle...")
	var certBundle server.CertificateBundle
	err = json.Unmarshal(resp.Bundle, &certBundle)
	if err != nil {
		return nil, errors.WithHint(
			errors.Wrap(err, "failed to unmarshal certificate bundle"),
			errJoinTokenConsumedHint)
	}

	return &certBundle, nil
}

// runConnectJoin will attempt to connect to peers from the join list provided and
// request a certificate initialization bundle if it is able to validate a
// peer.
// TODO(aaron-crl): Parallelize this and handle errors.
func runConnectJoin(cmd *cobra.Command, args []string) error {
	report := func(format string, args ...interface{}) {
		fmt.Printf(format+"\n", args...)
	}

	return contextutil.RunWithTimeout(
		context.Background(), "init handshake", nodeJoinTimeout,
		func(ctx context.Context) error {
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
				retryableErr, certPool, err := requestPeerCA(ctx, stopper, report, peer, jt)
				if err != nil {
					if retryableErr {
						// Try a different peer.
						log.Errorf(ctx, "failed requesting peer CA from %s: %s", peer, err)
						continue
					}
					return errors.Wrapf(err, "%q", peer)
				}

				// TODO(aaron-crl): Update add/join to signal to client when a token IS
				// consumed.
				certBundle, err := requestCertBundle(ctx, stopper, report, peer, certPool, jt)
				if err != nil {
					return errors.Wrapf(err, "%q", peer)
				}

				report("generating node certificates...")
				// Use the bundle to initialize the node.
				err = certBundle.InitializeNodeFromBundle(ctx, *baseCfg)
				if err != nil {
					return errors.WithHint(
						err,
						errJoinTokenConsumedHint,
					)
				}
				return nil
			}

			return errors.WithHint(
				errors.New("could not successfully authenticate with any listed nodes"),
				"Maybe review the --join flag and check that the nodes are running.")
		})
}

func validateNodeJoinFlags(_ *cobra.Command) error {
	if len(serverCfg.JoinList) == 0 {
		return errors.Newf("flag --%s must specify address of at least one node to join",
			cliflags.Join.Name)
	}
	return nil
}
