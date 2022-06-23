// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package interrupt

import (
	"context"
	"os"
	"os/signal"
)

var signals = append(
	[]os.Signal{
		os.Interrupt,
	},
	extraSignals...,
)

// WithCancel returns a context that is cancelled if interrupt signals are sent.
func WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	signalC, closer := NewSignalChannel()
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		<-signalC
		closer()
		cancel()
	}()
	return ctx, cancel
}

// NewSignalChannel returns a new channel for interrupt signals.
//
// Call the returned function to cancel sending to this channel.
func NewSignalChannel() (<-chan os.Signal, func()) {
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, signals...)
	return signalC, func() {
		signal.Stop(signalC)
		close(signalC)
	}
}
