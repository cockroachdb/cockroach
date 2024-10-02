// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames/bind";
import React from "react";

import styles from "./summaryCard.module.styl";

interface ISummaryCardProps {
  children: React.ReactNode;
  className?: string;
}

const cx = classnames.bind(styles);

export const SummaryCard: React.FC<ISummaryCardProps> = ({
  children,
  className = "",
}) => <div className={`${cx("summary--card")} ${className}`}>{children}</div>;

interface ISummaryCardItemProps {
  label: React.ReactNode;
  value: React.ReactNode;
  className?: string;
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
