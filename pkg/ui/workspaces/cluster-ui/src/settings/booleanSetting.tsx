// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import * as React from "react";

import { Tooltip } from "src/components/tooltip";
import { CircleFilled } from "src/icon";

import styles from "./booleanSetting.module.scss";

const cx = classNames.bind(styles);

export interface BooleanSettingProps {
  text: string;
  enabled: boolean;
  tooltipText: JSX.Element;
}

export function BooleanSetting(props: BooleanSettingProps): React.ReactElement {
  const { text, enabled, tooltipText } = props;
  const label = enabled ? "enabled" : "disabled";
  const boolClass = enabled
    ? "bool-setting-icon__enabled"
    : "bool-setting-icon__disabled";
  return (
    <div>
      <CircleFilled className={cx(boolClass)} />
      <Tooltip placement="bottom" title={tooltipText}>
        {text} - {label}
      </Tooltip>
    </div>
  );
}
