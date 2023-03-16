// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backgroundprofiler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// ProfileID is a unique identifier of the operation being profiled by the
// Profiler.
type ProfileID int

// SubscriberID is a unique identifier of the Subscriber subscribing to the
// background profile collection.
type SubscriberID int

// IsSet returns true if the BackgroundProfiler is currently associated with a
// profileID.
func (r ProfileID) IsSet() bool {
	return r != 0
}

// Subscriber is the interface that describes an object that can subscribe to
// the background profiler.
type Subscriber interface {
	// LabelValue returns the value that will be used when setting the pprof
	// labels of the Subscriber. The key of the label will always be the ProfileID
	// thereby allowing us to identify all samples that describe the operation
	// being profiled.
	LabelValue() string
	// Identifier returns the unique identifier of the Subscriber.
	Identifier() SubscriberID
	// ProfileID returns the unique identifier of the operation that the
	// Subscriber is executing on behalf of.
	ProfileID() ProfileID
}

// Profiler is the interface that exposes methods to subscribe and unsubscribe
// from a background profiler.
type Profiler interface {
	// Subscribe registers the subscriber with the background profiler. This
	// method returns a context wrapped with pprof labels along with a closure to
	// restore the original labels of the context.
	Subscribe(ctx context.Context, subscriber Subscriber) (context.Context, func())
	// Unsubscribe unregisters the subscriber from the background profiler. If the
	// subscriber is responsible for finishing the profile the method will also
	// return metadata describing the collected profile.
	Unsubscribe(subscriber Subscriber) (finishedProfile bool, msg protoutil.Message)
}
