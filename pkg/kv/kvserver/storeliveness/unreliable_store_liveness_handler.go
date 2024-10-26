// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"

type UnreliableHandlerFuncs struct {
	DropStoreLivenessMsg func(*storelivenesspb.Message) bool
}

// UnreliableHandler drops all StoreLiveness messages that are addressed to
// the specified storeID, but lets all other messages through.
type UnreliableHandler struct {
	Name string
	MessageHandler
	UnreliableHandlerFuncs
}

var _ MessageHandler = &UnreliableHandler{}

// HandleMessage implements the MessageHandler interface.
func (h *UnreliableHandler) HandleMessage(msg *storelivenesspb.Message) error {
	if h.DropStoreLivenessMsg != nil && h.DropStoreLivenessMsg(msg) {
		return nil
	}

	return h.MessageHandler.HandleMessage(msg)
}
