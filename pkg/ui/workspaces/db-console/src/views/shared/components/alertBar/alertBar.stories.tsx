// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { storiesOf } from "@storybook/react";
import moment from "moment";
import React from "react";

import { styledWrapper } from "src/util/decorators";

import { AlertBar } from "./alertBar";

storiesOf("AlertBar", module)
  .addDecorator(
    styledWrapper({ padding: "24px", fontFamily: "SourceSansPro-Regular" }),
  )
  .add(`3.4.1 [pre-license] license key required by <date>`, () => (
    <AlertBar
      license={"None"}
      numNodes={3}
      throttled={false}
      telemetrySuccessful={true}
      throttleDate={moment("2025-01-04")}
    />
  ))
  .add(`3.4.2 [pre-license] license key required, cluster throttled`, () => (
    <AlertBar
      license={"None"}
      numNodes={3}
      throttled={true}
      telemetrySuccessful={true}
      throttleDate={moment("2023-01-04")}
    />
  ))
  .add(
    `3.4.5 [license expiring] license key expired on <date>, cluster will be throttled on <date>`,
    () => (
      <AlertBar
        license={"Free"}
        numNodes={3}
        throttled={false}
        telemetrySuccessful={true}
        licenseExpiryDate={moment().subtract(1, "month")}
        throttleDate={moment().add(3, "day")}
      />
    ),
  )
  .add(
    `3.4.6 + 3.4.7 [license expired] license key expired on <date>, cluster throttled`,
    () => (
      <AlertBar
        license={"Free"}
        numNodes={3}
        throttled={true}
        telemetrySuccessful={true}
        licenseExpiryDate={moment().subtract(1, "month")}
        throttleDate={moment().subtract(1, "day")}
      />
    ),
  )
  .add(
    `3.4.8 [telemetry disabled] telemetry not received since <date>, cluster will be throttled on <date>`,
    () => (
      <AlertBar
        license={"Free"}
        numNodes={3}
        throttled={false}
        telemetrySuccessful={false}
        licenseExpiryDate={moment().add(6, "month")}
        lastSuccessfulTelemetryDate={moment().subtract(3, "day")}
        throttleDate={moment().add(3, "day")}
      />
    ),
  )
  .add(
    `3.4.9 [telemetry disabled] telemetry not received since <date>, cluster throttled on <date>`,
    () => (
      <AlertBar
        license={"Free"}
        numNodes={3}
        throttled={true}
        telemetrySuccessful={false}
        licenseExpiryDate={moment().add(6, "month")}
        lastSuccessfulTelemetryDate={moment().subtract(1, "month")}
        throttleDate={moment().subtract(3, "day")}
      />
    ),
  );
