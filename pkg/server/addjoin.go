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

	"github.com/cockroachdb/errors"

	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/google/uuid"
)

// JoinToken is a container for a TokenID and associated SharedSecret for use
// in certificate-free add/join operations.
type JoinToken struct {
	TokenID      uuid.UUID
	SharedSecret []byte
}

func requestPeerCA(ctx context.Context, peer string, jt JoinToken) ([]byte, error) {
	//dialOpts, err := ctx.GRPCDialOptions
	//if err != nil {
	//	return nil, err
	//}

	conn, err := grpc.DialContext(ctx, peer, nil)
	if err != nil {
		return nil, err
	}

	addJoinClient := serverpb.NewAddJoinClient(conn)

	caRequest := &serverpb.CaRequest{}
	callOpts := grpc.EmptyCallOption{}

	caResponse, err := addJoinClient.CA(ctx, caRequest, callOpts)
	if err != nil {
		return nil, err
	}

	// TODO(aaron-crl): Verify bundle is valid.
	if caResponse.MAC != nil {
		return caResponse.CaCert, nil
	}

	return nil, errors.New("invalid bundle signature")
}

// JoinToPeers will attempt to connect to peers from the list provided and
// request a certificate initialization bundle if it is able to validate a
// peer.
// TODO(aaron-crl): Parallelize this and handle errors.
func JoinToPeers(peers []string, jt JoinToken) error {
	ctx := context.Background()

	for _, peer := range peers {

		caCert, err := requestPeerCA(ctx, peer, jt)
		if err != nil {
			return errors.Wrap(
				err, "failed to join peer %q",
			)
		}

		// TODO(aaron-crl): Finish the handshake.
		log.Fatalf(ctx, "got valid caCert: %s", caCert)
	}

	return nil
}
