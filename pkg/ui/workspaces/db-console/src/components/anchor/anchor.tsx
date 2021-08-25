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
import classnames from "classnames/bind";
import styles from "./anchor.module.styl";

type AnchorProps = React.DetailedHTMLProps<
  React.AnchorHTMLAttributes<HTMLAnchorElement>,
  HTMLAnchorElement
>;

const cx = classnames.bind(styles);

export function Anchor({
  target = "_blank",
  className,
  children,
  ...props
}: AnchorProps) {
  return (
    <a {...props} className={cx("crl-anchor", className)} target={target}>
      {children}
    </a>
  );
}
