// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"

type UnreliableHandlerFuncs struct {
	DropStoreLivenessMsg func(*storelivenesspb.Message) bool
}

// UnreliableHandler allows users to selectively drop StoreLiveness messages.
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
