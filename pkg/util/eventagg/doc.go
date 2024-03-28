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
// library that standardizes the way in which we aggregate Observability event data in CRDB.
// The goal is to eventually emit that data as "exhaust" from CRDB, which downstream systems
// can consume to build Observability features that do not rely on CRDB's own availability to
// aid in debugging & investigations.
//
// ## Guiding Principles:
// The eventagg package has a few core guiding principles that inform our decision making in its design:
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
// Map/Reduce aggregators via the mapReduceFlushConsumer interface. You can find a couple examples
// (topK, log) of mapReduceFlushConsumer implementations in pkg/util/eventagg/flush_consumer.go.
//
// ## Making Map/Reduce Easy for Engineers
// As part of our guiding principle to make eventagg easy to use, Map/Reduce needs to be easy to use as
// the core building block of the entire package. Requiring engineers to provide a Map() and Reduce()
// function of their own seems like a reasonable amount of effort, but can we do better?
//
// The eventagg package aims to use code generation on the input types to the MapReduceAggregator. Using
// struct tags, we can define how fields can be merged across two events of the same type, as well as define
// fields which should be included as part of a composite key.
//
// This strategy allows us to avoid requiring users of the library from providing Map() and Reduce() functions
// altogether, and instead rely on struct tags and code generation to do it for them.
//
// ## Map/Reduce Consumer Plugins
// Users of the eventagg package should have a library of Map/Reduce output consumers available to make
// things easy to use. For example, after aggregating some data, I want to calculate the TopK elements
// based on some field. That should be as easy for engineers as instantiating a plugin and defining the
// field to sort on. Similarly, other common tasks should have available plugins, and creating a new
// plugin should be easy enough that an engineer feels empowered to build their own (if one doesn't already
// exist for their use case).
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
//	2. For the struct tags used for code generation, I want to find a way to derive merge operations that
//		 require additional data structures. For example recording a value to a histogram, merging into a set,
//		 etc.
//
// Additionally, while the POC in its current state is still focusing on core interfaces, there are some
// challenges that we're punting until later that are worth enumerating here.
//
// Unsolved Challenges:
//	1. What mechanism will be used to control aggregation windows used by Map/Reduce? Presumably, the data
//		 needs to align to a certain aggregation timestamp at a certain frequency (e.g. once per-minute). We
//		 will need to find a way to make this easy for engineers to define, where data is aligned at the
//		 boundaries of each aggregation window's timestamp. There are a few different ways we can approach
//		 this, but for now we punt the problem until later.
//	2. What do the details of the structured logging API look like that the eventagg package will use?
//		 Current telemetry logging requires protobuf messages which can be unwieldy and require us to either
//		 use protobuf types within the eventagg system (undesirable) or transform aggregation results into
//		 a protobuf type (duplicative). Some early ideas on how to solve this are discussed in comments within
//		 pkg/util/eventagg/flush_consumer.go, within (*LogWriteConsumer[T]).onFlush(). For now, we punt this
//		 problem until the broader eventagg interfaces are agreed upon.
// 	3. Memory accounting as well as observability into the aggregations themselves are required to make
//	   eventagg safe to use and easy to debug. For now, we punt this problem.
// 	4. What kinds of concurrency, if any, should be used within the eventagg package? If the goal is for
//		 eventagg to be easy for developers to use, then making a Map/Reduce operation concurrent is
//	 	 should also be easy if there's a need for it. We punt whether this is a requirement, and the details
//		 of the problem if it *is* a requirement, until later.
