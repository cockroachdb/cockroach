// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eventagg

// The eventagg package is (currently) a proof of concept ("POC") that aims to provide an easy-to-use
// library that standardizes the way in which we aggregate Observability events data in CRDB.
// The goal is to eventually emit that data as "exhaust" from CRDB, which downstream systems
// can consume to build Observability features that do not rely on CRDB's own availability to
// aid in debugging & investigations. Finally, we aim to provide engineers with the means to use
// that same exhaust within CRDB to act as input that additional features can be built on top of.
//
// ## Guiding Principles:
// The eventagg package has a few core guiding principles that inform our decision-making in its design:
//		1. eventagg should require minimal effort by engineers to use, otherwise it won't get used at all.
//		2. eventagg should be architected in a way flexible enough where engineers are able to create their
// 			 own plugins to expand the package's functionality, if they choose to do so.
//		3. eventagg should be easy to reason about with respect to resource consumption and runtime operations.
//
// ## Aggregations:
// The POC rallies around a single aggregation mechanism to serve as the core component to all other
// types of aggregations: Map/Reduce. From the Map/Reduce output, we can compute additional aggregations
// (for example) TopK, simply log the results as structured events, or apply additional processing.
//
// The package aims to allow engineers to create new plugins that can consume these outputs from a
// Map/Reduce aggregators via the flushConsumer interface. You can find examples of flushConsumer
// implementations in pkg/util/eventagg/flush_consumer.go.
//
// ## Making Map/Reduce Easy for Engineers
// As part of our guiding principle to make eventagg easy to use, Map/Reduce needs to be easy to use as
// the core building block of the entire package. It will require engineers to implement types that the
// aggregation will operate on, and an interface that effectively allows us to perform Map/Reduce-like
// operations on those types.
//
// ## Map/Reduce Consumer Plugins
// Users of the eventagg package should have a library of Map/Reduce output consumers available to make
// things easy to use. For example, after aggregating some data, I want to calculate the TopK elements
// based on some field. That should be as easy for engineers as instantiating a plugin and defining the
// field to sort on. Similarly, other common tasks should have available plugins, and creating a new
// plugin should be easy enough that an engineer feels empowered to build their own (if one doesn't already
// exist for their use case).
//
// ## Flushing Map/Reduce Aggregations
// Users of the eventagg package have the ability to use various implementations of the FlushTrigger interface.
// FlushTrigger is used each time an event is passed to a MapReduceAggregator, to determine whether a flush should
// be performed of the currently buffered data before aggregating the next event.
//
// As an example, a WindowedFlush implementation is provided, which aligns aggregation intervals to truncated
// time intervals [startTime, endTime). The downside of this approach is that it relies on event consumption to
// trigger a flush, meaning an arbitrary amount of time may pass beyond the intended end time of the aggregation
// window until things are flushed. In an environment where events are expected to have a regular frequency, this
// should not be too burdensome. The ability to create solutions for this is possible via new FlushTrigger
// implementations, but we punt this for now to avoid slowing prototyping progress.
//
// ## Wish List & Unsolved Challenges
// Through the exercise of building this proof of concept, I came across some wishlist items that require
// further thinking.
//
// Wish list:
//	1. Ability to chain Map/Reduce consumer plugins. The reason this isn't possible currently is because we
//		 pass each consumer the entire set of data (e.g. a map[k]v, a heap, etc). The idea here is that this
//		 provides more flexibility to the consumers to operate on the full dataset, as opposed to just being
//		 fed a single element at a time using a Visitor() pattern. I'd like to find a way to have both here -
//		 the ability for consumers to be fed the raw data structure used in the previous aggregation, and the
//		 ability to somehow chain together consumers regardless of the type of that data structure.
//	2. Code generation via struct tags, to handle implementing the methods of the Mergeable[K, T] interface.
//
// Additionally, while the POC in its current state is still focusing on core interfaces, there are some
// challenges that we're punting until later that are worth enumerating here.
//
// Unsolved Challenges:
// 	1. Memory accounting as well as observability into the aggregations themselves are required to make
//	   eventagg safe to use and easy to debug. For now, we punt this problem.
// 	2. What kinds of concurrency, if any, should be used within the eventagg package? If the goal is for
//		 eventagg to be easy for developers to use, then making a Map/Reduce operation concurrent is
//	 	 should also be easy if there's a need for it. We punt whether this is a requirement, and the details
//		 of the problem if it *is* a requirement, until later.
