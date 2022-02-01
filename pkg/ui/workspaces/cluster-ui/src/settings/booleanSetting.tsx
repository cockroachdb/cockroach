// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";
import { CircleFilled } from "src/icon";
import { Tooltip } from "antd";
import classNames from "classnames/bind";
import styles from "./booleanSetting.module.scss";

const cx = classNames.bind(styles);

export interface BooleanSettingProps {
  text: string;
  enabled: boolean;
  tooltipText: JSX.Element;
}

export function BooleanSetting(props: BooleanSettingProps) {
  const { text, enabled, tooltipText } = props;
  if (enabled) {
    return (
      <div>
        <CircleFilled className={cx("bool-setting-icon__enabled")} />
        <Tooltip
          placement="bottom"
          title={tooltipText}
          className={cx("crl-hover-text__dashed-underline")}
        >
          {text} - enabled
        </Tooltip>
      </div>
    );
  }
  return (
    <div>
      <CircleFilled className={cx("bool-setting-icon__disabled")} />
      <Tooltip
        placement="bottom"
        title={tooltipText}
        className={cx("crl-hover-text__dashed-underline")}
      >
        {text} - disabled
      </Tooltip>
    </div>
  );
}
