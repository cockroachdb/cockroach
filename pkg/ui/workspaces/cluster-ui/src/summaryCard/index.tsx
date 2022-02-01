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
import classnames from "classnames/bind";
import styles from "./summaryCard.module.scss";
import booleanSettingStyles from "../settings/booleanSetting.module.scss";
import { CircleFilled } from "src/icon";
import { Tooltip } from "antd";

interface ISummaryCardProps {
  children: React.ReactNode;
  className?: string;
}

const cx = classnames.bind(styles);
const booleanSettingCx = classnames.bind(booleanSettingStyles);

// tslint:disable-next-line: variable-name
export const SummaryCard: React.FC<ISummaryCardProps> = ({
  children,
  className = "",
}) => <div className={`${cx("summary--card")} ${className}`}>{children}</div>;

interface ISummaryCardItemProps {
  label: React.ReactNode;
  value: React.ReactNode;
  className?: string;
}

interface ISummaryCardItemBoolSettingProps extends ISummaryCardItemProps {
  toolTipText: JSX.Element;
}

export const SummaryCardItem: React.FC<ISummaryCardItemProps> = ({
  label,
  value,
  className = "",
}) => (
  <div className={cx("summary--card__item", className)}>
    <h4 className={cx("summary--card__item--label")}>{label}</h4>
    <p className={cx("summary--card__item--value")}>{value}</p>
  </div>
);

export const SummaryCardItemBoolSetting: React.FC<ISummaryCardItemBoolSettingProps> = ({
  label,
  value,
  toolTipText,
  className,
}) => {
  const boolValue = value ? "Enabled" : "Disabled";
  const boolClass = value
    ? "bool-setting-icon__enabled"
    : "bool-setting-icon__disabled";

  return (
    <div className={cx("summary--card__item", className)}>
      <h4 className={cx("summary--card__item--label")}>{label}</h4>
      <p className={cx("summary--card__item--value")}>
        <CircleFilled className={booleanSettingCx(boolClass)} />
        <Tooltip
          placement="bottom"
          title={toolTipText}
          className={cx("crl-hover-text__dashed-underline")}
        >
          {boolValue}
        </Tooltip>
      </p>
    </div>
  );
};
