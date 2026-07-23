// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import moment from "moment-timezone";
import React, { useState } from "react";

import { TimeScaleDropdown } from "./timeScaleDropdown";
import { defaultTimeScaleOptions, defaultTimeScaleSelected } from "./utils";

export function TimeScaleDropdownWrapper({
  initialTimeScale = defaultTimeScaleSelected,
}): React.ReactElement {
  const [timeScale, setTimeScale] = useState(initialTimeScale);
  return (
    <TimeScaleDropdown currentScale={timeScale} setTimeScale={setTimeScale} />
  );
}

storiesOf("TimeScaleDropdown", module)
  .add("default", () => <TimeScaleDropdownWrapper />)
  .add("custom", () => (
    <TimeScaleDropdownWrapper
      initialTimeScale={{
        sampleSize: defaultTimeScaleOptions["Past 6 Hours"].sampleSize,
        windowSize: moment.duration(6, "h"),
        fixedWindowEnd: moment().subtract(10, "m"),
        key: "Custom",
      }}
    />
  ));
