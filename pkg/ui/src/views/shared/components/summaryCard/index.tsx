// Copyright 2018 The Cockroach Authors.
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
import styles from  "./summaryCard.module.styl";

interface ISummaryCardProps {
  children: React.ReactNode;
  className?: string;
}

const cx = classnames.bind(styles);

// tslint:disable-next-line: variable-name
export const SummaryCard: React.FC<ISummaryCardProps> = ({ children, className = "" }) => (
  <div className={`${cx("summary--card")} ${className}`}>
    {children}
  </div>
);

interface ISummaryCardItemProps {
  label: React.ReactNode;
  value: React.ReactNode;
}

export const SummaryCardItem: React.FC<ISummaryCardItemProps> = ( {label, value }) => (
<div className={cx("summary--card__item")}>
  <h4 className={cx("summary--card__item--label")}>{ label }</h4>
  <p className={cx("summary--card__item--value")}>{ value }</p>
</div>
);
