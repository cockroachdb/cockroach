// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdctest

//
//import (
//	"context"
//
//	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
//)
//
//type MockPubsubMessage struct {
//	Message string
//	Err     error
//}
//
//// MockPubsubSink is the Webhook sink used in tests.
//type MockPubsubSink struct {
//}
//
//// MakeMockPubsubSink returns a MockPubsubSink object initialized with the given url and context
//func MakeMockPubsubSink(url string) (*MockPubsubSink, error) {
//	ctx := context.Background()
//	ctx, shutdown := context.WithCancel(ctx)
//	groupCtx := ctxgroup.WithContext(ctx)
//	p := &MockPubsubSink{
//		ctx: ctx, MessageChan: make(chan messageStruct, 1), url: url, shutdown: shutdown,
//		groupCtx: groupCtx,
//	}
//	return p, nil
//}
