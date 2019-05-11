// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import Long from "long";
import * as protos from "src/js/protos";

// For testing only.

const static_num_nodes: number = 50;

const static_components: any[] = [
  {name: "gateway.pgwire.clients", freq: 72},
  {name: "gateway.sql.optimizer", freq: 70},
  {name: "gateway.sql.executor", freq: 70},
  {name: "gateway.dist.txn.refresh", freq: 5},
  {name: "gateway.dist.txn.retry", freq: 7},
  {name: "gateway.dist.txn.commit", freq: 25},
  {name: "gateway.dist.range lookup", freq: 2},
  {name: "gateway.dist.divider", freq: 50},
  {name: "gateway.dist.sender", freq: 50},
  {name: "storage.store.send", freq: 90},
  {name: "storage.store.retry", freq: 5},
  {name: "storage.store.intent resolution", freq: 20},
  {name: "storage.store.txn wait", freq: 3},
  {name: "storage.store.txn contention", freq: 2},
  {name: "storage.store.coalesced heartbeat", freq: 7},
  {name: "storage.store.liveness", freq: 4},
  {name: "storage.replica.evaluator.admin", freq: 1},
  {name: "storage.replica.evaluator.read-only", freq: 70},
  {name: "storage.replica.evaluator.read-write", freq: 25},
  {name: "storage.replica.latch manager", freq: 90},
  {name: "storage.replica.lease acquisition", freq: 5},
  {name: "storage.replica.raft proposal", freq: 100},
  {name: "storage.replica.split", freq: 7},
  {name: "storage.replica.merge", freq: 3},
  {name: "storage.replica.up-replicate", freq: 5},
  {name: "storage.replica.membership", freq: 5},
  {name: "storage.raft.snapshot", freq: 4},
  {name: "storage.raft.heartbeat", freq: 30},
  {name: "storage.raft.apply", freq: 75},
  {name: "storage.queues.consistency", freq: 1},
  {name: "storage.queues.gc", freq: 1},
  {name: "storage.queues.merge", freq: 1},
  {name: "storage.queues.raft log", freq: 25},
  {name: "storage.queues.raft snapshot", freq: 5},
  {name: "storage.queues.replica gc", freq: 1},
  {name: "storage.queues.replicate", freq: 5},
  {name: "storage.queues.split", freq: 5},
  {name: "storage.queues.merge", freq: 2},
  {name: "storage.queues.compaction", freq: 1},
  {name: "storage.queues.ts maintenance", freq: 1},
  {name: "time series.db.query", freq: 15},
  {name: "time series.db.rollup", freq: 2},
  {name: "time series.db.prune", freq: 2},
  {name: "gossip.network.membership", freq: 10},
  {name: "gossip.network.connectivity", freq: 15},
];

function newRandomLong() {
  return new Long(Math.random() * 0x7FFFFFFF, Math.random() * 0x7FFFFFFF);
}

function getCurrentTime() {
  const now: number = Date.now();
  return new protos.google.protobuf.Timestamp({
    seconds: Long.fromNumber(Math.floor(now / 1000)),
    nanos: (now % 1000) * 1000000,
  });
}

function getRandomDuration() {
  return new protos.google.protobuf.Duration({
    seconds: Math.floor(Math.random() * 2),
    nanos: 1000000 * Math.floor(Math.random() * 1000),
  });
}

function createRandomLogLines(start_time: protos.google.protobuf.Timestamp,
                              duration: protos.google.protobuf.Duration) {
  const dur_nanos: number = duration.seconds * 1E9 + duration.nanos;
  const num_logs: number = Math.floor(Math.random() * 20);
  var offset_nanos: number = 0;
  var logs: protos.cockroach.util.tracing.RecordedSpan.LogRecord[] = [];

  for (let i = 0; i < num_logs; i++) {
    offset_nanos += Math.floor(Math.random() * (dur_nanos - offset_nanos) / (num_logs - i));
    var cur_time: protos.google.protobuf.Timestamp = new protos.google.protobuf.Timestamp({
      seconds: start_time.seconds.add(0),
      nanos: start_time.nanos
    });
    cur_time.nanos += offset_nanos;
    if (cur_time.nanos > 1E9) {
      cur_time.seconds = cur_time.seconds.add(Math.floor(cur_time.nanos / 1E9));
      cur_time.nanos = cur_time.nanos % 1E9;
    }
    logs.push(new protos.cockroach.util.tracing.RecordedSpan.LogRecord({
      time: cur_time,
      fields: [new protos.cockroach.util.tracing.RecordedSpan.LogRecord.Field({
        key: "event",
        value: "log message " + i
      })],
    }));
  }
  return logs;
}

function genChildSpans(depth: number, addSample: function, root_span: protos.cockroach.util.tracing.RecordedSpan) {
  const dur_nanos: number = root_span.duration.seconds * 1E9 + root_span.duration.nanos;
  const num_spans: number = Math.floor(Math.random() * 6 / depth);
  var offset_nanos: number = 0;
  var spans: protos.cockroach.util.tracing.RecordedSpan[] = [];

  for (let i = 0; i < num_spans; i++) {
    offset_nanos += Math.floor(Math.random() * (dur_nanos - offset_nanos) / (num_spans - i));
    var cur_time: protos.google.protobuf.Timestamp = new protos.google.protobuf.Timestamp({
      seconds: root_span.start_time.seconds.add(0),
      nanos: root_span.start_time.nanos
    });
    cur_time.nanos += offset_nanos;
    if (cur_time.nanos > 1E9) {
      cur_time.seconds = cur_time.seconds.add(Math.floor(cur_time.nanos / 1E9));
      cur_time.nanos = cur_time.nanos % 1E9;
    }
    var duration = new protos.google.protobuf.Duration(
      {seconds: new Long(0, 0), nanos: Math.floor(Math.random() * (dur_nanos - offset_nanos) / (num_spans - i))});

    // Create a new sample or a child span.
    if (Math.random() < 0.5) {
      spans.push(...genSpans(addSample, null, depth + 1, root_span.trace_id, root_span.span_id, cur_time, duration));
    } else {
      genSample(Math.floor(Math.random() * static_num_nodes + 1),
                static_components[Math.floor(Math.random() * static_components.length)].name,
                addSample, depth + 1, root_span.trace_id, root_span.span_id, cur_time, duration);
    }
  }
  return spans;
}

function genSpans(addSample: function,
                  component: string,
                  depth: number,
                  trace_id: Long, parent_span_id: Long,
                  start_time: protos.google.protobuf.Timestamp,
                  duration: protos.google.protobuf.Duration) {
  if (!start_time) {
    start_time = getCurrentTime();
  }
  if (!duration) {
    duration = getRandomDuration();
  }

  const root_span: protos.cockroach.util.tracing.RecordedSpan = new protos.cockroach.util.tracing.RecordedSpan({
    trace_id: trace_id ? trace_id : newRandomLong(),
    span_id: newRandomLong(),
    parent_span_id: parent_span_id,
    start_time: start_time,
    duration: duration,
    operation: component ? component + ": op" : "op";
    logs: createRandomLogLines(start_time, duration),
  });
  var spans: protos.cockroach.util.tracing.RecordedSpan[] = [root_span];
  spans.push(...genChildSpans(depth, addSample, root_span));
  return spans;
}

function genSample(node_id: number, component: string, addSample: function,
                   depth: number,
                   trace_id?: Long, parent_span_id?: Long,
                   start_time?: protos.google.protobuf.ITimestamp,
                   duration?: protos.google.protobuf.IDuration) {
  addSample(node_id, component, new protos.cockroach.util.tracing.ComponentSamples.Sample({
    error: Math.random() < 0.10 ? "Random error message" : null,
    pending: Math.random() < 0.05 ? true : false,
    stuck: Math.random() < 0.0001 ? true : false,
    attributes: {
      replica_id: Math.floor(Math.random() * 1000000),
      store_id: Math.floor(Math.random() * 20),
      input: "Random input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value\nRandom input value",
      output: "Random output value"
    },
    spans: genSpans(addSample, component, depth, trace_id, parent_span_id, start_time, duration),
  }));
}

export function genComponentTraces(node_id: number, components: string[], sampleMap: any) {
  var traces: any = {};
  components.forEach((component) => {
    traces[component] = new protos.cockroach.util.tracing.ComponentTraces({
      samples: new protos.cockroach.util.tracing.ComponentSamples({samples: sampleMap[node_id][component]}),
      timestamp: sampleMap.timestamp
    });
  });
  return traces;
}

export function genComponentTrace(trace_id: Long, sampleMap: any) {
  var nodes: protos.cockroach.server.serverpb.ComponentTrace_INodeResponse[] = [];
  _.map(sampleMap, (val, node_id) => {
    if (node_id == 'timestamp') {
      return;
    }
    var result: {[component: string]: protos.cockroach.util.tracing.IComponentSamples} = {};
    _.map(val, (samples, component) => {
      samples.forEach((s) => {
        if (trace_id.equals(s.spans[0].trace_id)) {
          if (!(component in result)) {
            result[component] = new protos.cockroach.util.tracing.ComponentSamples({samples: []});
          }
          result[component].samples.push(s);
        }
      });
    });
    nodes.push(new protos.cockroach.server.serverpb.ComponentTraceResponse.NodeResponse({
      node_id: node_id,
      samples: result
    }));
  });
  return nodes;
}

export function genSampleMap() {
  var sampleMap: any = {}
  const addSample = function(node_id: number, component: string, sample: protos.cockroach.util.tracing.ComponentActivity.Sample) {
    if (!(node_id in sampleMap)) {
      sampleMap[node_id] = {};
    }
    if (!(component in sampleMap[node_id])) {
      sampleMap[node_id][component] = [];
    }
    sampleMap[node_id][component].push(sample);
  }

  for (let node_id = 1; node_id <= static_num_nodes; node_id++) {
    _.map(static_components, (c) => {
      const num_samples: number = Math.floor(Math.random() * 50);
      const start: number = ((node_id in sampleMap) && c.name in sampleMap[node_id]) ? sampleMap[node_id][c.name].length : 0;
      for (let i = start; i < num_samples; i++) {
        genSample(node_id, c.name, addSample, 1);
      }
    });
  };
  sampleMap['timestamp'] = getCurrentTime();

  return sampleMap;
}

function genActivityResponse(node_id: number) {
  if (Math.random() < 0.02) {
    return new protos.cockroach.server.serverpb.ComponentsResponse.NodeResponse(
      {node_id: node_id, error_message: "no route to host"});
  }
  const compActivities: {[k: string]: protos.cockroach.util.tracing.IComponentActivity} = {};
  _.map(static_components, (c) => {
    if (Math.random() < 0.05) {
      return null;
    }
    compActivities[c.name] = new protos.cockroach.util.tracing.ComponentActivity(
      {
        span_count: Long.fromNumber(Math.max(2, Math.floor(Math.random() * c.freq - c.freq/2) + c.freq)),
        stuck_count: Long.fromNumber(Math.random() < 0.0001 ? Math.floor(Math.random() * 3) : 0),
        errors: Long.fromNumber(Math.random() < 0.02 * (c.freq / 100) ? Math.floor(Math.random() * 5) : 0),
        timestamp: getCurrentTime(),
      });
  });
  return new protos.cockroach.server.serverpb.ComponentsResponse.NodeResponse(
    {node_id: node_id, components: compActivities});
}

export function genActivityResponses() {
  var resps: protos.cockroach.server.serverpb.ComponentsResponse.NodeResponse[] =
    Array(static_num_nodes).fill(0).map((e,i)=>genActivityResponse(i+1));
  return resps;
}
