// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { mount, ReactWrapper } from "enzyme";
import moment from "moment";
import React from "react";

import { AlertBar } from "src/views/shared/components/alertBar/alertBar";

describe("AlertBar", () => {
  let wrapper: ReactWrapper;

  it("displays nothing if no grace period and no telemetry deadline and 3 nodes", () => {
    wrapper = mount(
      <AlertBar
        hasGracePeriod={false}
        hasTelemetryDeadline={false}
        numNodes={3}
        license={"None"}
        throttled={false}
      />,
    );

    expect(wrapper.isEmptyRender());
  });

  it("displays nothing if 1 node even if grace period is present", () => {
    wrapper = mount(
      <AlertBar
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        numNodes={1}
        license={"Free"}
        throttled={false}
      />,
    );

    expect(wrapper.isEmptyRender());
  });

  it("displays throttle message: grace period = elapsed, license = expired, cluster = throttled", () => {
    wrapper = mount(
      <AlertBar
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        numNodes={3}
        license={"Free"}
        throttled={true}
        licenseExpiryDate={moment("2024-09-15")}
        gracePeriodEnd={moment("2024-09-07")}
      />,
    );

    expect(wrapper.text()).toContain(
      "Your license key expired on September 15th, 2024 and the cluster was throttled. " +
        "Please add a license key to continue using this cluster. Learn more",
    );
  });

  it("displays warning message: grace period = active, license = expired, cluster = not throttled", () => {
    const gracePeriodEnd = moment().add(1, "days");
    wrapper = mount(
      <AlertBar
        hasGracePeriod={true}
        hasTelemetryDeadline={true}
        numNodes={3}
        license={"Free"}
        throttled={false}
        licenseExpiryDate={moment("2024-09-15")}
        gracePeriodEnd={gracePeriodEnd}
      />,
    );

    expect(wrapper.text()).toContain(
      "Your license key expired on September 15th, 2024. " +
        `The cluster will be throttled on ${gracePeriodEnd.format("MMMM Do, YYYY")} unless a new license key is added. Learn more`,
    );
  });

  it("displays throttle message: telemetry = missing, license = active, cluster = throttled", () => {
    wrapper = mount(
      <AlertBar
        hasGracePeriod={false}
        hasTelemetryDeadline={true}
        numNodes={3}
        nodesMissingTelemetry={["1", "2", "3"]}
        license={"Free"}
        throttled={true}
        lastSuccessfulTelemetryDate={moment("2024-09-01")}
        telemetryDeadline={moment("2024-09-15")}
      />,
    );

    expect(wrapper.text()).toContain(
      "Telemetry has not been received from some nodes in this cluster since " +
        "September 1st, 2024. These nodes were throttled on September 15th, 2024. Learn more",
    );
  });

  it("displays warning message: telemetry = missing recently, license = active, cluster = not throttled", () => {
    const telemetryDeadline = moment().add(1, "days");
    wrapper = mount(
      <AlertBar
        hasGracePeriod={false}
        hasTelemetryDeadline={true}
        numNodes={3}
        nodesMissingTelemetry={["1", "2", "3"]}
        license={"Free"}
        throttled={false}
        lastSuccessfulTelemetryDate={moment("2024-09-01")}
        telemetryDeadline={telemetryDeadline}
      />,
    );

    expect(wrapper.text()).toContain(
      "Telemetry has not been received from some nodes in this cluster since " +
        `September 1st, 2024. These nodes will be throttled on ${telemetryDeadline.format("MMMM Do, YYYY")} unless telemetry is received. Learn more`,
    );
  });

  it("displays throttle message: license = None, cluster = throttled", () => {
    wrapper = mount(
      <AlertBar
        hasGracePeriod={true}
        hasTelemetryDeadline={false}
        numNodes={3}
        license={"None"}
        throttled={true}
      />,
    );

    expect(wrapper.text()).toContain(
      "This cluster was throttled because it requires a license key. Please add a license key to continue using this cluster. Learn more",
    );
  });
  it("displays warning message: license = None, cluster = not throttled", () => {
    const gracePeriodEnd = moment().add(1, "days");
    wrapper = mount(
      <AlertBar
        hasGracePeriod={true}
        hasTelemetryDeadline={false}
        numNodes={3}
        license={"None"}
        throttled={false}
        gracePeriodEnd={gracePeriodEnd}
      />,
    );

    expect(wrapper.text()).toContain(
      `This cluster will require a license key by ${gracePeriodEnd.format("MMMM Do, YYYY")} or the cluster will be throttled. Learn more`,
    );
  });
});
