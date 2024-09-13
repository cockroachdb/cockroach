// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Spinner, SpinnerProps } from "@cockroachlabs/ui-components";
import classnames from "classnames/bind";
import React from "react";

import styles from "./delayedLoaderIcon.module.scss";
import { useDelay } from "./useDelay";

const cx = classnames.bind(styles);

interface DelayedLoaderIconProps {
  size?: SpinnerProps["size"];
  className?: string;
}

const DelayedLoaderIcon = ({ size, className }: DelayedLoaderIconProps) => {
  // Anything that takes less than 500s doesn't require special feedback,
  // so there is no need to show a loader. We originally tried 1s limit
  // from https://www.nngroup.com/articles/response-times-3-important-limits/,
  // but it was noticeably long.
  const instantaneousLimitReached = useDelay(500);

  if (instantaneousLimitReached) {
    return <Spinner className={cx("spinner", className)} size={size} />;
  }

  return null;
};

export default DelayedLoaderIcon;
