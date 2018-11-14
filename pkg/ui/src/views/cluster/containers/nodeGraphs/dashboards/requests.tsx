// Copyright 2018 The Cockroach Authors.
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

import React from "react";

import { LineGraph } from "src/views/cluster/components/linegraph";
import { Metric, Axis } from "src/views/shared/components/metricQuery";

import { GraphDashboardProps } from "./dashboardUtils";

export default function (props: GraphDashboardProps) {
  const { storeSources } = props;

  return [
    <LineGraph title="Slow Raft Proposals" sources={storeSources}>
      <Axis label="proposals">
        <Metric name="cr.store.requests.slow.raft" title="Slow Raft Proposals" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Lease Acquisitions" sources={storeSources}>
      <Axis label="lease acquisitions">
        <Metric name="cr.store.requests.slow.lease" title="Slow Lease Acquisitions" downsampleMax />
      </Axis>
    </LineGraph>,

    <LineGraph title="Slow Command Queue Entries" sources={storeSources}>
      <Axis label="queue entries">
        <Metric name="cr.store.requests.slow.commandqueue" title="Slow Command Queue Entries" downsampleMax />
      </Axis>
    </LineGraph>,
  ];
}
