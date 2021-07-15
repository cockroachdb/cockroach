// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type TestChan struct {
	s *stop.Stopper
	c chan interface{}
}

// NewTestChan is a wrapper around a `chan interface{}` that facilitates
// writing robust tests.
//
// 1. error handling for send and receive when the stopper quiesces
// 2. timeout-enforcing API.
//
// These help solve a common problem, namely that test failures often turn
// into hanging packages or ... TBD
func NewTestChan(s *stop.Stopper, cap int) *TestChan {
	ch := &TestChan{
		s: s,
		c: make(chan interface{}, cap),
	}
	return ch
}

func (ch *TestChan) RecvD(timeout time.Duration) (interface{}, error) {
	select {
	case data := <-ch.c:
		return data, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout reached")
	case <-ch.s.ShouldQuiesce():
		return nil, errors.New("stopper quiescing")
	}
}

func (ch *TestChan) Recv() (interface{}, error) {
	return ch.RecvD(DefaultSucceedsSoonDuration)
}

func (ch *TestChan) SendD(timeout time.Duration, data interface{}) error {
	select {
	case ch.c <- data:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout reached")
	case <-ch.s.ShouldQuiesce():
		return errors.New("stopper is quiescing")
	}
}

func (ch *TestChan) Send(data interface{}) error {
	return ch.SendD(DefaultSucceedsSoonDuration, data)
}

func (ch *TestChan) Close() {
	close(ch.c)
}
