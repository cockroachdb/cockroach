// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames/bind";
import React from "react";

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
