// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";
import { RenderFunction } from "storybook__react";

import { withRouterDecorator } from "src/util/decorators";

import {
  latencyFixture,
  latencyFixtureWithNodeStatuses,
} from "./latency.fixtures";

import { Latency } from "./index";

storiesOf("Latency Table", module)
  .addDecorator(withRouterDecorator)
  .addDecorator((storyFn: RenderFunction) => (
    <div style={{ marginLeft: "1rem" }}>{storyFn()}</div>
  ))
  .add("Healthy nodes with connection problems", () => (
    <>
      <h3>Network partitioned with healthy nodes</h3>
      <ul>
        <li>
          <b>n1-n2, n1-n3</b> - failed connection
        </li>
        <li>
          <b>n2-n4, n2-n5</b> - not connected
        </li>
        <li>
          <b>n3-n1, n3-n2</b> - unknown status
        </li>
        <li>
          <b>n4-n1, n4-n2</b> - closed connection
        </li>
      </ul>
      <Latency {...latencyFixture} />
    </>
  ))
  .add("Unhealthy nodes", () => (
    <>
      <h3>Different node statuses</h3>
      <ul>
        <li>
          <b>n1</b> - live node
        </li>
        <li>
          <b>n2</b> - dead node
        </li>
        <li>
          <b>n3</b> - unavailable node
        </li>
        <li>
          <b>n4</b> - decommissioned node
        </li>
        <li>
          <b>n5</b> - decommissioning node
        </li>
      </ul>
      <Latency {...latencyFixtureWithNodeStatuses} />
    </>
  ));
