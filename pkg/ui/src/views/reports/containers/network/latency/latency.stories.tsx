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
import { Latency } from "./index";
import { latencyFixture, latencyFixtureNoLocality } from "./latency.fixtures";
import { withRouterDecorator } from "src/util/decorators";

storiesOf("Latency Table", module)
  .addDecorator(withRouterDecorator)
  .add("Default state", () => <Latency {...latencyFixture} />)
  .add("No localites state", () => <Latency {...latencyFixtureNoLocality} />);
