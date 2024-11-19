// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import styles from "./pageLayout.module.scss";

const cx = classNames.bind(styles);

export const PageLayout: React.FC = ({ children }) => {
  return <div className={cx("page-layout")}>{children}</div>;
};
