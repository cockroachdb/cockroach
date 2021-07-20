// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

/*
Package changefeedccl is the internal implementation behind
changefeeds.

Changefeeds emit KV events on user-specified tables to user-specified
sinks.

Changefeeds are built on top of rangefeeds, which provide a stream of
KV events for a given keyspan as well as periodic "resolved
timestamps" for those spans. For more information on rangefeeds see

    docs/RFCS/20170613_range_feeds_storage_primitive.md

The changefeed machinery encodes and delivers both the KV events
and resolved timestamps to the sinks. It further uses the resolved
timestamps to periodically checkpoint a changefeed's progress such
that it can be resumed in the case of a failure.

To ensure that we can correctly encode every KV returned by the
rangefeed, changefeeds also monitor for schema changes.

"Enterprise" changefeeds are all changefeeds with a sink. These
feeds emit KV events to external systems and are run via the job
system.

"Sinkless" or "Experimental" changefeeds are changefeeds without a
sink which emit rows back to the original sql node that issues the
CREATE CHANGEFEED request.

The major components of this system are:

changfeedAggregator: Reads events from a kvfeed, encodes and emits
KV events to the sink and forwards resolved to the changeFrontier.

changeFrontier: Keeps track of the high-watermark of resolved
timestamps seen across the spans we are tracking. Periodically, it
emits resolved timestamps to the sink and checkpoints the
changefeed progress in the job system.

kvfeed: Coordinates the consumption of the rangefeed with the
schemafeed. It starts a set of goroutines that consume the
rangefeed events and forwards events back to the
changefeedAggregator once the schema for the event is known.

schemafeed: Periodically polls the table descriptors
table. Rangefeed events are held until it is sure it knows the
schema for the relevant table at the event's timestamp.

                 +-----------------+
  +------+       |                 |       +-----+
  | sink |<------+  changeFrontier +------>| job |
  +------+       |                 |       +-----+
                 +--------+--------+
                          ^
                          |
                  +-------+--------+
  +------+        |                |
  | sink +<-------+  changefeedAgg |<------------+
  +------+        |                |             |
                  +--+-------------+         chanBuffer
                     |                           |
                     v                    +------+------+
                   +--------------+       |             |
                   |              +------>|  copyFromTo +--+
                   |  kvfeed      |       |             |  |
                   |              |       +------+------+  |
                   +--------+---+-+              ^         |
                            |   |             memBuffer    |
                            |   |                |         |
                            |   |          +-----+------+  |   +-----------+
                            |   |          |            |  |   |           |
                            |   +--------> |physical    +----->| rangefeed |
                            |              |   feed     |  |   |           |
                            |              +------------+  |   +-----------+
                            |                              |
                            |                              |
                            |              +------------+  |
                            +------------> | schemafeed |<-|
                                           |  (polls)   |
                                           +------------+

*/
package changefeedccl
