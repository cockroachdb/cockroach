// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Link as LinkTo, LinkProps } from "react-router-dom";
import classnames from "classnames/bind";
import styles from "./link.module.styl";

const cx = classnames.bind(styles);

export function Link({ className, children, to }: LinkProps) {
  return (
    <LinkTo className={cx("crl-link", className)} to={to}>
      {children}
    </LinkTo>
  );
}
