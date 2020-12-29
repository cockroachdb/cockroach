// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
