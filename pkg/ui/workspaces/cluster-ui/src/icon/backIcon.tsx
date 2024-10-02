// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React from "react";

import Back from "./back-arrow.svg";
import styles from "./backIcon.module.scss";

const cx = classNames.bind(styles);

export const BackIcon = (): React.ReactElement => (
  <img src={Back} alt="back" className={cx("root")} />
);
