// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/pkg/errors"
)

type kvDebugServer struct {
	s *Server
}

type clientInterface roachpb.KVDebug_TxnCoordSenderServer

func (*kvDebugServer) manage(c clientInterface, sender client.Sender) error {
	ctx := c.Context()
	if peer, ok := peer.FromContext(ctx); ok {
		if tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo); ok {
			certUser, err := security.GetCertificateUser(&tlsInfo.State)
			if err != nil {
				return err
			}
			if certUser != security.NodeUser {
				return errors.Errorf("user %s is not allowed", certUser)
			}
		}
	}
	for {
		ba, err := c.Recv()
		if err != nil {
			return err
		}
		br, pErr := sender.Send(ctx, *ba)
		if pErr != nil {
			br = &roachpb.BatchResponse{}
			br.Error = pErr
		}
		if err := c.Send(br); err != nil {
			return err
		}
	}
}

func (ds *kvDebugServer) TxnCoordSender(c roachpb.KVDebug_TxnCoordSenderServer) error {
	return ds.manage(c, ds.s.txnCoordSender)
}

func (ds *kvDebugServer) EvalEngine(c roachpb.KVDebug_EvalEngineServer) error {
	sender := client.SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		rs, err := keys.Range(ba)
		if err != nil {
			return nil, roachpb.NewError(err)
		}

		repDesc := ba.Header.Replica
		rangeID := ba.Header.RangeID
		if repDesc.StoreID == 0 {
			var err error
			rangeID, repDesc, err = ds.s.node.stores.LookupReplica(rs.Key, rs.EndKey)
			if err != nil {
				return nil, roachpb.NewError(err)
			}
		}
		store, err := ds.s.node.stores.GetStore(repDesc.StoreID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		repl, err := store.GetReplica(rangeID)
		if err != nil {
			return nil, roachpb.NewError(err)
		}
		rawSender := storage.RawEngineSender{
			Repl:  repl,
			Apply: true,
		}
		return rawSender.Send(ctx, ba)
	})
	return ds.manage(c, sender)
}
