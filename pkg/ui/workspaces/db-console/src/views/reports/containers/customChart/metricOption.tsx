// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { OptionComponentProps } from "react-select";
import classnames from "classnames";

import "./metricOption.styl";

export function MetricOption(props: OptionComponentProps) {
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
