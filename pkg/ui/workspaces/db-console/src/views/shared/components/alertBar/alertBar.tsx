// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classNames from "classnames/bind";
import moment from "moment";
import React from "react";

import { enterpriseLicenseUpdate } from "src/util/docs";

import styles from "./alertBar.module.styl";

type License = "Enterprise" | "Free" | "None";

interface AlertBarProps {
  license: License;
  numNodes: number;
  throttled: boolean;
  telemetrySuccessful: boolean;
  licenseExpiryDate?: moment.Moment;
  lastSuccessfulTelemetryDate?: moment.Moment;
  throttleDate?: moment.Moment;
  nodesMissingTelemetry?: [number];
}

const cx = classNames.bind(styles);

export const AlertBar = ({
  license,
  numNodes,
  throttled,
  telemetrySuccessful,
  licenseExpiryDate,
  lastSuccessfulTelemetryDate,
  throttleDate,
}: AlertBarProps) => {
  if (license === "Enterprise" || numNodes < 2) {
    return null; // No matter what, an enterprise license is never throttled.
  }

  if (license === "Free") {
    // If license is expired, we throttle.
    if (licenseExpiryDate.isBefore(moment())) {
      if (throttled) {
        return (
          <div className={cx("alert-bar", "alert--alert")}>
            Your license key expired on{" "}
            {licenseExpiryDate.format("MMMM Do, YYYY")} and the cluster was
            throttled. Please add a license key to continue using this cluster.{" "}
            <a href={enterpriseLicenseUpdate}>Learn more</a>
          </div>
        );
      } else {
        return (
          <div className={cx("alert-bar", "alert--warning")}>
            Your license key expired on{" "}
            {licenseExpiryDate.format("MMMM Do, YYYY")}. The cluster will be
            throttled on {throttleDate.format("MMMM Do, YYYY")} unless the
            license is renewed. <a href={enterpriseLicenseUpdate}>Learn more</a>
          </div>
        );
      }
    }
    if (!telemetrySuccessful) {
      if (throttled) {
        return (
          <div className={cx("alert-bar", "alert--alert")}>
            Telemetry has not been received from some nodes in this cluster
            since {lastSuccessfulTelemetryDate.format("MMMM Do, YYYY")}. These
            nodes were throttled on {throttleDate.format("MMMM Do, YYYY")}.{" "}
            <a href={enterpriseLicenseUpdate}>Learn more</a>
          </div>
        );
      } else {
        return (
          <div className={cx("alert-bar", "alert--warning")}>
            Telemetry has not been received from some nodes in this cluster
            since {lastSuccessfulTelemetryDate.format("MMMM Do, YYYY")}. These
            nodes will be throttled on {throttleDate.format("MMMM Do, YYYY")}{" "}
            unless telemetry is received.{" "}
            <a href={enterpriseLicenseUpdate}>Learn more</a>
          </div>
        );
      }
    }
  }

  if (license === "None") {
    if (throttled) {
      return (
        <div className={cx("alert-bar", "alert--alert")}>
          This cluster was throttled because it requires a license key. Please
          add a license key to continue using this cluster.{" "}
          <a href={enterpriseLicenseUpdate}>Learn more</a>
        </div>
      );
    } else {
      return (
        <div className={cx("alert-bar", "alert--warning")}>
          This cluster will require a license key by{" "}
          {throttleDate.format("MMMM Do, YYYY")} or the cluster will be
          throttled. <a href={enterpriseLicenseUpdate}>Learn more</a>
        </div>
      );
    }
  }
};
