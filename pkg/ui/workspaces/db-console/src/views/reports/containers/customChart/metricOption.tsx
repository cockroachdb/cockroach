// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames";
import React from "react";
import { OptionProps, components } from "react-select";

import "./metricOption.scss";

interface MetricOptionData {
  label: string;
  value: string;
  description?: string;
}

export const MetricOption = React.memo(
  (props: OptionProps<MetricOptionData, false>) => {
    const { data } = props;
    const { label, description } = data;
    const classes = classnames("metric-option", {
      "metric-option--is-focused": props.isFocused,
      "metric-option--is-selected": props.isSelected,
    });

    return (
      <components.Option {...props}>
        <div
          className={classes}
          role="option"
          aria-label={label}
          title={description}
        >
          <div className="metric-option__label">{label}</div>
          {description && (
            <div className="metric-option__description" title={description}>
              {description}
            </div>
          )}
        </div>
      </components.Option>
    );
  },
);
