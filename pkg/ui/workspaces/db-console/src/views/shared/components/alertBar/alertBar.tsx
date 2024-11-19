// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import moment from "moment";
import React, { useEffect, useState } from "react";
import { useSelector } from "react-redux";

import { Tooltip } from "src/components";
import {
  daysUntilLicenseExpiresSelector,
  LicenseType,
  licenseTypeSelector,
} from "src/redux/alerts";
import { nodeIDsSelector } from "src/redux/nodes";
import {
  getThrottlingMetadata,
  GetThrottlingMetadataResponse,
} from "src/util/api";
import { throttlingFaqs } from "src/util/docs";

import styles from "./alertBar.module.styl";

interface AlertBarProps {
  license: LicenseType;
  numNodes: number;
  throttled: boolean;

  hasGracePeriod: boolean;
  licenseExpiryDate?: moment.Moment;
  gracePeriodEnd?: moment.Moment;

  hasTelemetryDeadline: boolean;
  lastSuccessfulTelemetryDate?: moment.Moment;
  telemetryDeadline?: moment.Moment;
  nodesMissingTelemetry?: string[];
}

const cx = classNames.bind(styles);

export const ThrottleNotificationBar = () => {
  const [loaded, updateLoaded] = useState(false);
  const daysUntilLicenseExpires = useSelector(daysUntilLicenseExpiresSelector);
  const licenseType = useSelector(licenseTypeSelector);
  const nodeIDs = useSelector(nodeIDsSelector);
  const init: GetThrottlingMetadataResponse = {
    throttled: false,
    throttleExplanation: "",
    hasGracePeriod: false,
    gracePeriodEndSeconds: undefined,
    hasTelemetryDeadline: false,
    telemetryDeadlineSeconds: undefined,
    lastTelemetryReceivedSeconds: undefined,
    nodeIdsWithTelemetryProblems: [],
  };
  const [throttleMetadata, updateThrottleMetadata] = useState(init);

  useEffect(() => {
    if (!loaded) {
      getThrottlingMetadata().then(resp => {
        updateThrottleMetadata(resp);
        updateLoaded(true);
      });
    }
  }, [loaded]);

  if (loaded) {
    return (
      <AlertBar
        license={licenseType}
        numNodes={nodeIDs.length}
        throttled={throttleMetadata.throttled}
        hasGracePeriod={throttleMetadata.hasGracePeriod}
        licenseExpiryDate={moment().add(daysUntilLicenseExpires, "days")}
        gracePeriodEnd={moment.unix(
          throttleMetadata.gracePeriodEndSeconds.toNumber(),
        )}
        hasTelemetryDeadline={throttleMetadata.hasTelemetryDeadline}
        lastSuccessfulTelemetryDate={moment.unix(
          throttleMetadata.lastTelemetryReceivedSeconds.toNumber(),
        )}
        telemetryDeadline={moment.unix(
          throttleMetadata.telemetryDeadlineSeconds.toNumber(),
        )}
        nodesMissingTelemetry={throttleMetadata.nodeIdsWithTelemetryProblems}
      />
    );
  } else {
    return null;
  }
};

export const AlertBar = ({
  license,
  numNodes,
  throttled,
  hasGracePeriod,
  licenseExpiryDate,
  gracePeriodEnd,
  hasTelemetryDeadline,
  lastSuccessfulTelemetryDate,
  telemetryDeadline,
  nodesMissingTelemetry,
}: AlertBarProps) => {
  // If there's no grace period, or telemetry deadline, or the cluster
  // is single-node, never show anything.
  if (
    (!throttled && !hasGracePeriod && !hasTelemetryDeadline) ||
    numNodes < 2
  ) {
    return null;
  }

  // If the license has a grace period (i.e. it's a trial/free
  // license), and it's expired, we will show a warning or alert based
  // on throttle status.
  if (hasGracePeriod && licenseExpiryDate?.isBefore(moment())) {
    if (throttled || gracePeriodEnd?.isSameOrBefore(moment())) {
      return (
        <div className={cx("alert-bar", "alert--alert")}>
          Your license key expired on{" "}
          {licenseExpiryDate.format("MMMM Do, YYYY")} and the cluster was
          throttled. Please add a license key to continue using this cluster.{" "}
          <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    } else {
      return (
        <div className={cx("alert-bar", "alert--warning")}>
          Your license key expired on{" "}
          {licenseExpiryDate.format("MMMM Do, YYYY")}. The cluster will be
          throttled on {gracePeriodEnd.format("MMMM Do, YYYY")} unless a new
          license key is added. <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    }
  }

  // If the license has a telemetry deadline (i.e. it's a trial/free
  // license), and it's been >1d since we received telemetry, we will
  // show a warning or alert based on throttle status.
  if (
    hasTelemetryDeadline &&
    lastSuccessfulTelemetryDate?.isBefore(moment().subtract(1, "day"))
  ) {
    const someNodes = (
      <Tooltip
        placement="bottom"
        title={
          <p>
            Nodes failing to send telemetry:
            <br />
            {nodesMissingTelemetry.sort().join(", ")}
          </p>
        }
      >
        <span className={cx("alert-tooltip")}>some nodes in this cluster</span>
      </Tooltip>
    );

    if (throttled || telemetryDeadline?.isSameOrBefore(moment())) {
      return (
        <div className={cx("alert-bar", "alert--alert")}>
          Telemetry has not been received from {someNodes} since{" "}
          {lastSuccessfulTelemetryDate.format("MMMM Do, YYYY")}. These nodes
          were throttled on {telemetryDeadline.format("MMMM Do, YYYY")}.{" "}
          <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    } else {
      return (
        <div className={cx("alert-bar", "alert--warning")}>
          Telemetry has not been received from {someNodes} since{" "}
          {lastSuccessfulTelemetryDate.format("MMMM Do, YYYY")}. These nodes
          will be throttled on {telemetryDeadline.format("MMMM Do, YYYY")}{" "}
          unless telemetry is received. <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    }
  }
  if (license === "None") {
    if (throttled) {
      return (
        <div className={cx("alert-bar", "alert--alert")}>
          This cluster was throttled because it requires a license key. Please
          add a license key to continue using this cluster.{" "}
          <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    } else {
      return (
        <div className={cx("alert-bar", "alert--warning")}>
          This cluster will require a license key by{" "}
          {gracePeriodEnd.format("MMMM Do, YYYY")} or the cluster will be
          throttled. <a href={throttlingFaqs}>Learn more</a>
        </div>
      );
    }
  }

  return null;
};
