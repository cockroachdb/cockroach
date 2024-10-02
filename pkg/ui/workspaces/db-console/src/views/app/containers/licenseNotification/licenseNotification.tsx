// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "antd";
import classNames from "classnames/bind";
import moment from "moment";
import React from "react";
import { useSelector } from "react-redux";

import ErrorIcon from "assets/error-circle.svg";
import InfoIcon from "assets/info-filled-circle.svg";
import WarningIcon from "assets/warning.svg";
import {
  daysUntilLicenseExpiresSelector,
  isManagedClusterSelector,
  licenseTypeSelector,
} from "src/redux/alerts";
import { licensingFaqs } from "src/util/docs";

import styles from "./licenseNotification.module.styl";

const cn = classNames.bind(styles);

export const LicenseNotification = () => {
  const daysToWarn = 30; // Equal to trial period
  const daysUntilLicenseExpires = useSelector(daysUntilLicenseExpiresSelector);
  const licenseType = useSelector(licenseTypeSelector);
  const isManagedCluster = useSelector(isManagedClusterSelector);

  if (licenseType === "None") {
    return null;
  }
  // Do not show notification for clusters that run as part of managed service.
  if (isManagedCluster) {
    return null;
  }
  let Icon: string;
  let title: React.ReactNode;
  let tooltipContent: JSX.Element;

  const learnMore = (
    <a
      href={licensingFaqs}
      target="_blank"
      rel="noreferrer"
      className={cn("tooltip-link")}
    >
      Learn more
    </a>
  );
  const expirationDateFormatted = moment()
    .add(daysUntilLicenseExpires, "days")
    .format("MMMM Do, YYYY");

  if (daysUntilLicenseExpires > daysToWarn) {
    title = (
      <span>
        License expires in <b>{Math.abs(daysUntilLicenseExpires)} days</b>
      </span>
    );
    Icon = InfoIcon;
    tooltipContent = (
      <span>
        You are using an {licenseType} license of CockroachDB. To avoid service
        disruption, please renew before the expiration date of{" "}
        {expirationDateFormatted}. {learnMore}
      </span>
    );
  } else if (daysUntilLicenseExpires === 0) {
    title = (
      <span>License expires in {Math.abs(daysUntilLicenseExpires)} days.</span>
    );
    Icon = WarningIcon;
    tooltipContent = (
      <span>
        Your {licenseType} license of CockroachDB expired today. To re-enable
        Enterprise features, please renew your license. {learnMore}
      </span>
    );
  } else if (daysUntilLicenseExpires <= 0) {
    title = (
      <span>
        <b>License expired</b> {Math.abs(daysUntilLicenseExpires)} days ago
      </span>
    );
    Icon = ErrorIcon;
    tooltipContent = (
      <span>
        Your {licenseType} license of CockroachDB expired on{" "}
        {expirationDateFormatted}. To re-enable Enterprise features, please
        renew your license. {learnMore}
      </span>
    );
  } else if (daysUntilLicenseExpires <= daysToWarn) {
    title = (
      <span>
        License expires in <b>{daysUntilLicenseExpires} days</b>
      </span>
    );
    Icon = WarningIcon;
    tooltipContent = (
      <span>
        You are using an {licenseType} license of CockroachDB. To avoid service
        disruption, please renew before the expiration date of{" "}
        {expirationDateFormatted}. {learnMore}
      </span>
    );
  }

  return (
    <Tooltip
      placement="bottom"
      title={tooltipContent}
      className={cn("container")}
    >
      <img src={Icon} className={cn("icon")} />
      <span className={cn("content")}>{title}</span>
    </Tooltip>
  );
};
