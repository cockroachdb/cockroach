// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames/bind";
import React from "react";
import { Link as LinkTo, LinkProps } from "react-router-dom";

import styles from "./link.module.styl";

const cx = classnames.bind(styles);

export function Link({ className, children, to }: LinkProps) {
  return (
    <LinkTo className={cx("crl-link", className)} to={to}>
      {children}
    </LinkTo>
  );
}
