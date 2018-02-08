// Copyright 2015 The Cockroach Authors.
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

package client

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// TxnType specifies whether a transaction is the root (parent)
// transaction, or a leaf (child) in a tree of client.Txns, as
// is used in a DistSQL flow.
type TxnType int

const (
	_ TxnType = iota
	// RootTxn specifies this sender is the root transaction, and is
	// responsible for aggregating all transactional state (see
	// TxnCoordMeta) and finalizing the transaction. The root txn is
	// responsible for heartbeating the transaction record.
	RootTxn
	// LeafTxn specifies this sender is for one of potentially many
	// distributed client transactions. The state from this transaction
	// must be propagated back to the root transaction and used to
	// augment its state before the transaction can be finalized. Leaf
	// transactions do not heartbeat the transaction record.
	//
	// Note: As leaves don't perform heartbeats, the transaction might be
	// cleaned up while this leaf is executing an operation. So data read
	// by a leaf txn is not guaranteed to not miss writes performed by the
	// transaction before the cleanup (at least not after the expiration
	// of the GC period / abort span entry timeout). If the client cares
	// about this hazard, the state of the heartbeats should be checked
	// using the root txn before delivering results to the client. DistSQL
	// does this.
	LeafTxn
)

// Sender is the interface used to call into a CockroachDB instance.
// If the returned *roachpb.Error is not nil, no response should be
// returned.
type Sender interface {
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

// TxnSender is the interface used to call into a CockroachDB instance
// when sending transactional requests. In addition to the usual
// Sender interface, TxnSender facilitates marshaling of transaction
// metadata between the "root" client.Txn and "leaf" instances.
type TxnSender interface {
	Sender

	// GetMeta retrieves a copy of the TxnCoordMeta, which can be sent
	// upstream in situations where there are multiple, leaf TxnSenders,
	// to be combined via AugmentMeta().
	GetMeta() roachpb.TxnCoordMeta
	// AugmentMeta combines the TxnCoordMeta from another distributed
	// TxnSender which is part of the same transaction.
	AugmentMeta(meta roachpb.TxnCoordMeta)
	// OnFinish invokes the supplied closure when the sender has finished
	// with the txn (i.e. it's been abandoned, aborted, or committed).
	// The error passed is meant to indicate to an extant distributed
	// SQL receiver that the underlying transaction record has either been
	// aborted (and why), or been committed. Only one callback is set, so
	// if this method is invoked multiple times, the most recent callback
	// is the only one which will be invoked.
	OnFinish(func(error))
}

// TxnSenderFactory is the interface used to create new instances
// of TxnSender.
type TxnSenderFactory interface {
	// New returns a new instance of TxnSender. The typ parameter
	// specifies whether the sender is the root or one of potentially
	// many child "leaf" nodes in a tree of transaction objects, as is
	// created during a DistSQL flow.
	New(typ TxnType) TxnSender
	// WrappedSender returns the TxnSenderFactory's wrapped Sender.
	WrappedSender() Sender
}

// SenderFunc is an adapter to allow the use of ordinary functions
// as Senders.
type SenderFunc func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send calls f(ctx, c).
func (f SenderFunc) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// TxnSenderFunc is an adapter to allow the use of ordinary functions
// as TxnSenders with GetMeta or AugmentMeta panicing with unimplemented.
// This is a helper mechanism to facilitate testing.
type TxnSenderFunc func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send calls f(ctx, c).
func (f TxnSenderFunc) Send(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// GetMeta is part of the TxnSender interface.
func (f TxnSenderFunc) GetMeta() roachpb.TxnCoordMeta { panic("unimplemented") }

// AugmentMeta is part of the TxnSender interface.
func (f TxnSenderFunc) AugmentMeta(_ roachpb.TxnCoordMeta) { panic("unimplemented") }

// OnFinish is part of the TxnSender interface.
func (f TxnSenderFunc) OnFinish(_ func(error)) { panic("unimplemented") }

// TxnSenderFactoryFunc is an adapter to allow the use of ordinary functions
// as TxnSenderFactories. This is a helper mechanism to facilitate testing.
type TxnSenderFactoryFunc func(TxnType) TxnSender

// New calls f().
func (f TxnSenderFactoryFunc) New(typ TxnType) TxnSender {
	return f(typ)
}

// WrappedSender is not implemented for TxnSenderFactoryFunc.
func (f TxnSenderFactoryFunc) WrappedSender() Sender {
	panic("unimplemented")
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it via the provided Sender and headers. It returns the unwrapped
// response or an error. It's valid to pass a `nil` context; an empty one is
// used in that case.
func SendWrappedWith(
	ctx context.Context, sender Sender, h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.Add(args)

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	unwrappedReply := br.Responses[0].GetInner()
	header := unwrappedReply.Header()
	header.Txn = br.Txn
	unwrappedReply.SetHeader(header)
	return unwrappedReply, nil
}

// SendWrapped is identical to SendWrappedWith with a zero header.
// TODO(tschottdorf): should move this to testutils and merge with
// other helpers which are used, for example, in `storage`.
func SendWrapped(
	ctx context.Context, sender Sender, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	return SendWrappedWith(ctx, sender, roachpb.Header{}, args)
}

// Wrap returns a Sender which applies the given function before delegating to
// the supplied Sender.
func Wrap(sender Sender, f func(roachpb.BatchRequest) roachpb.BatchRequest) Sender {
	return SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(ctx, f(ba))
	})
}
