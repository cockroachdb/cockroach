// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import classnames from "classnames";
import React from "react";
import { OptionComponentProps } from "react-select";

import "./metricOption.styl";

export const MetricOption = (props: OptionComponentProps<string>) => {
  const { option, className, onSelect, onFocus } = props;
  const { label, description } = option;
  const classes = classnames("metric-option", className);

  return (
    <div
      className={classes}
      role="option"
      aria-label={label}
      title={option.title}
      onMouseDown={event => onSelect(option, event)}
      onMouseEnter={event => onFocus(option, event)}
    >
      <div className="metric-option__label">{label}</div>
      <div className="metric-option__description" title={description}>
        {description}
      </div>
    </div>
  );
}
