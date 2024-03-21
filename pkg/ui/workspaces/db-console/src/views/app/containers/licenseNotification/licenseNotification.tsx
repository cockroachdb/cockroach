// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import { useSelector } from "react-redux";
import classNames from "classnames/bind";
import {
  daysUntilLicenseExpiresSelector,
  licenseTypeSelector,
} from "src/redux/alerts";
import moment from "moment";
import ErrorIcon from "assets/error-circle.svg";
import InfoIcon from "assets/info-filled-circle.svg";
import WarningIcon from "assets/warning.svg";
import styles from "./licenseNotification.module.styl";
import { licensingFaqs } from "src/util/docs";

const cn = classNames.bind(styles);

export const LicenseNotification = () => {
  const daysToWarn = 30; // Equal to trial period
  const daysUntilLicenseExpires = useSelector(daysUntilLicenseExpiresSelector);
  const licenseType = useSelector(licenseTypeSelector);

  if (licenseType === "None") {
    return null;
  }
  let Icon: string;
  let title: React.ReactNode;
  let tooltipContent: JSX.Element;

  if (daysUntilLicenseExpires > daysToWarn) {
    title = (
      <span>
        License expires in <b>{Math.abs(daysUntilLicenseExpires)}</b> days
      </span>
    );
    Icon = InfoIcon;
    tooltipContent = (
      <span>
        License expires on{" "}
        {moment().add(daysUntilLicenseExpires, "days").format("MMMM Do, YYYY")}.
        <br />
        <a
          href={licensingFaqs}
          target="_blank"
          rel="noreferrer"
          className={cn("tooltip-link")}
        >
          Learn more
        </a>{" "}
        how to renew.
      </span>
    );
  } else if (daysUntilLicenseExpires < 0) {
    title = (
      <span>
        <b>License expired</b> {Math.abs(daysUntilLicenseExpires)} days ago
      </span>
    );
    Icon = ErrorIcon;
    tooltipContent = (
      <span>
        You are using {licenseType} license of CockroachDB. <br />
        To avoid service disruption, please renew it.{" "}
        <a
          href={licensingFaqs}
          target="_blank"
          rel="noreferrer"
          className={cn("tooltip-link")}
        >
          Learn more
        </a>{" "}
        how to renew.
      </span>
    );
  } else if (daysUntilLicenseExpires === 0) {
    title = <span>License expired</span>;
    Icon = ErrorIcon;
    tooltipContent = (
      <span>
        You are using {licenseType} license of CockroachDB. <br />
        To avoid service disruption, please renew it.{" "}
        <a
          href={licensingFaqs}
          target="_blank"
          rel="noreferrer"
          className={cn("tooltip-link")}
        >
          Learn more
        </a>{" "}
        how to renew.
      </span>
    );
  } else if (daysUntilLicenseExpires <= daysToWarn) {
    title = (
      <span>
        License expires in <b>{daysUntilLicenseExpires}</b> days
      </span>
    );
    Icon = WarningIcon;
    tooltipContent = (
      <span>
        You are using {licenseType} license of CockroachDB. <br />
        To avoid service disruption, please renew before the expiration date of{" "}
        {moment()
          .add(daysUntilLicenseExpires, "days")
          .format("MMMM Do, YYYY")}.{" "}
        <a
          href={licensingFaqs}
          target="_blank"
          rel="noreferrer"
          className={cn("tooltip-link")}
        >
          Learn more
        </a>{" "}
        how to renew.
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
