// Copyright 2021 The Cockroach Authors.
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
import { start, end, allowedInterval } from "./dateRange.fixtures";
import { DateRange } from "./index";

storiesOf("DateRange", module)
  .add("default", () => (
    <DateRange
      allowedInterval={allowedInterval}
      end={end}
      onSubmit={() => {}}
      start={start}
    />
  ))
  .add("with invalid date range", () => (
    <DateRange
      allowedInterval={allowedInterval}
      end={start}
      onSubmit={() => {}}
      start={end}
    />
  ));
