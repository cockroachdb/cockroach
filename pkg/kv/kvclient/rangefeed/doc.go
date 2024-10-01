// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rangefeed provides a useful client abstraction atop of the rangefeed
// functionality exported by the DistSender.
//
// In particular, the abstraction exported by this package hooks up a stopper,
// and deals with retries upon errors, tracking resolved timestamps along the
// way.
package rangefeed

// TODO(ajwerner): Rework this logic to encapsulate the multi-span logic in
// changefeedccl/kvfeed. That code also deals with some schema interactions but
// it should be split into two layers. The primary limitation missing here is
// just the ability to watch multiple spans however the way that the KV feed
// manages internal state and sometimes triggers re-scanning would require some
// interface changes.
