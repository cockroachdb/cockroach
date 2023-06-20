// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";
import { RenderFunction } from "storybook__react";
import { Latency } from "./index";
import {
  latencyFixture,
  latencyFixtureWithNodeStatuses,
} from "./latency.fixtures";
import { withRouterDecorator } from "src/util/decorators";

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
