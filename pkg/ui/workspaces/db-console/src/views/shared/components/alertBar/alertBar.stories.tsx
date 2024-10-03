// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
      hasTelemetryDeadline={true}
      gracePeriodEnd={moment("2025-01-04")}
      hasGracePeriod={true}
    />
  ))
  .add(`3.4.2 [pre-license] license key required, cluster throttled`, () => (
    <AlertBar
      license={"None"}
      numNodes={3}
      throttled={true}
      hasTelemetryDeadline={true}
      gracePeriodEnd={moment("2024-01-04")}
      hasGracePeriod={true}
    />
  ))
  .add(
    `3.4.5 [license expiring] license key expired on <date>, cluster will be throttled on <date>`,
    () => (
      <AlertBar
        license={"Free"}
        numNodes={3}
        throttled={false}
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        licenseExpiryDate={moment().subtract(1, "month")}
        gracePeriodEnd={moment().add(3, "day")}
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
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        licenseExpiryDate={moment().subtract(1, "month")}
        gracePeriodEnd={moment().subtract(3, "day")}
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
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        licenseExpiryDate={moment().add(1, "month")}
        gracePeriodEnd={moment().add(2, "month")}
        lastSuccessfulTelemetryDate={moment().subtract(2, "day")}
        telemetryDeadline={moment().add(2, "day")}
        nodesMissingTelemetry={["1", "2", "3"]}
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
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        licenseExpiryDate={moment().add(1, "month")}
        gracePeriodEnd={moment().add(2, "month")}
        lastSuccessfulTelemetryDate={moment().subtract(9, "day")}
        telemetryDeadline={moment().subtract(2, "day")}
        nodesMissingTelemetry={["1", "2", "3"]}
      />
    ),
  );
