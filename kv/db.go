// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// KV is an interface for routing key-value DB commands. This
// package includes both local and distributed KV implementations.
type KV interface {
	// ExecuteCmd executes the specified command (method + args) and
	// sends a reply on the provided channel.
	ExecuteCmd(method string, args proto.Request, replyChan interface{})
	// Close provides implementation-specific cleanup.
	Close()
}

// A DB is the kv package implementation of the storage.DB
// interface. Each of its methods creates a channel for the response
// then invokes executeCmd on the underlying KV interface. KV
// implementations include local (DB) and distributed (DistKV). A
// kv.DB incorporates transaction management.
type DB struct {
	*storage.BaseDB
	kv          KV           // Local or distributed KV implementation
	clock       *hlc.Clock   // Time signal
	coordinator *coordinator // Coordinates transaction states for clients
}

// NewDB returns a key-value implementation of storage.DB which
// connects to the Cockroach cluster via the supplied kv.KV instance.
func NewDB(kv KV, clock *hlc.Clock) *DB {
	db := &DB{
		kv:    kv,
		clock: clock,
	}
	db.BaseDB = storage.NewBaseDB(db.executeCmd)
	db.coordinator = newCoordinator(db, db.clock)
	return db
}

// Close calls coordinator.Close(), stopping any ongoing transaction
// heartbeats.
func (db *DB) Close() {
	db.coordinator.Close()
	db.kv.Close()
}

// executeCmd adds the request to the transaction coordinator and
// passes the command to the underlying KV implementation.
func (db *DB) executeCmd(method string, args proto.Request, replyChan interface{}) {
	db.coordinator.AddRequest(method, args.Header())
	db.kv.ExecuteCmd(method, args, replyChan)
}

// BeginTransaction starts a transaction by initializing a new
// Transaction proto using the contents of the request. Note that this
// method does not call through to the key value interface but instead
// services it directly, as creating a new transaction requires only
// access to the node's clock. Nothing must be read or written.
func (db *DB) BeginTransaction(args *proto.BeginTransactionRequest) <-chan *proto.BeginTransactionResponse {
	txn := storage.NewTransaction(args.Name, args.Key, args.GetUserPriority(), args.Isolation, db.clock)
	reply := &proto.BeginTransactionResponse{
		ResponseHeader: proto.ResponseHeader{
			Timestamp: txn.Timestamp,
		},
		Txn: txn,
	}
	replyChan := make(chan *proto.BeginTransactionResponse, 1)
	replyChan <- reply
	return replyChan
}

// EndTransaction either commits or aborts an ongoing transaction by
// executing an EndTransaction command on the KV. The reply is
// intercepted here in order to inform the transaction coordinator of
// the final state of the transaction.
func (db *DB) EndTransaction(args *proto.EndTransactionRequest) <-chan *proto.EndTransactionResponse {
	interceptChan := make(chan *proto.EndTransactionResponse, 1)
	replyChan := make(chan *proto.EndTransactionResponse, 1)
	go func() {
		db.Executor(storage.EndTransaction, args, interceptChan)
		// Intercept the reply and end transaction on coordinator
		// depending on final state.
		reply := <-interceptChan
		if reply.Error == nil && reply.Txn.Status != proto.PENDING {
			db.coordinator.EndTxn(reply.Txn)
		}
		// Go ahead and return the result to the client.
		replyChan <- reply
	}()
	return replyChan
}

// RunTransaction executes retryable in the context of a distributed
// transaction.
// TODO(Spencer): write or copy a more descriptive comment here as various
// references to this comment exist.
func (db *DB) RunTransaction(opts *storage.TransactionOptions, retryable func(db storage.DB) error) error {
	return runTransaction(db, opts, retryable)
}
