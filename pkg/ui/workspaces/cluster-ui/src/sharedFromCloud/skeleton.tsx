// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Skeleton as AntDSkeleton } from "antd";
import classnames from "classnames/bind";
import React from "react";

import styles from "./skeleton.module.scss";
const cx = classnames.bind(styles);

interface SkeletonProps {
  width?: string;
  height?: string;
  ariaLabel?: string;
  ariaLabelledBy?: string;
  style?: React.CSSProperties;
  className?: string;
}

export const Skeleton = ({
  width,
  height,
  ariaLabelledBy,
  ariaLabel,
  className,
}: SkeletonProps) => {
  // We use the skeleton input due to it having the most flexible
  // API for setting up custom widths and layouts. These commonly
  // have aria attributes for testing and accessibility.
  return (
    <div
      className={cx("skeleton", className)}
      aria-label={ariaLabel}
      aria-labelledby={ariaLabelledBy}
      style={{
        width: width,
        height: height,
      }}
    >
      <AntDSkeleton.Input active />
    </div>
  );
};
