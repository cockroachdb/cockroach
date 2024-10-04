// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { storiesOf } from "@storybook/react";
import { Latency } from "./index";
import { latencyFixture, latencyFixtureNoLocality } from "./latency.fixtures";
import { withRouterDecorator } from "src/util/decorators";

storiesOf("Latency Table", module)
  .addDecorator(withRouterDecorator)
  .add("Default state", () => <Latency {...latencyFixture} />)
  .add("No localites state", () => <Latency {...latencyFixtureNoLocality} />);
