// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Tooltip } from "antd";
import React from "react";

import { Text, TextTypes } from "src/components";
import { Chip } from "src/views/app/components/chip";
import "./legend.styl";

interface ILegendProps {
  stddevMinus2: number;
  stddevMinus1: number;
  mean: number;
  stddevPlus1: number;
  stddevPlus2: number;
}

export const Legend: React.SFC<ILegendProps> = ({
  stddevMinus2,
  stddevMinus1,
  mean,
  stddevPlus1,
  stddevPlus2,
}) => (
  <div key="legend" className="Legend">
    <div className="Legend--container">
      <div className="Legend--container__head">
        <Tooltip
          placement="bottom"
          title="This legend represents the standard deviation of network latencies across all nodes in your cluster. It will help you understand if there are high latencies across nodes or regions."
        >
          <h3 className="Legend--container__head--title">Standard Deviation</h3>
        </Tooltip>
      </div>
      <div className="Legend--container__body">
        <div className="Legend--container__body--element">
          <Chip title={`${stddevMinus2.toFixed(2)}ms`} type="green" />
          <span className="Legend--container__body--label">
            <Text textType={TextTypes.BodyStrong}>{`-2`}</Text>&nbsp;
            <Text
              textType={TextTypes.Body}
              className="Legend--container__body--label-suffix"
            >
              std dev
            </Text>
          </span>
        </div>
        <div className="Legend--container__body--element">
          <Chip title={`${stddevMinus1.toFixed(2)}ms`} type="lightgreen" />
          <span className="Legend--container__body--label">
            <Text textType={TextTypes.BodyStrong}>{`-1`}</Text>&nbsp;
            <Text
              textType={TextTypes.Body}
              className="Legend--container__body--label-suffix"
            >
              std dev
            </Text>
          </span>
        </div>
        <div className="Legend--container__body--element">
          <Chip title={`${mean.toFixed(2)}ms`} type="grey" />
          <Text
            textType={TextTypes.BodyStrong}
            className="Legend--container__body--label"
          >
            Mean
          </Text>
        </div>
        <div className="Legend--container__body--element">
          <Chip title={`${stddevPlus1.toFixed(2)}ms`} type="lightblue" />
          <span className="Legend--container__body--label">
            <Text textType={TextTypes.BodyStrong}>{`+1`}</Text>&nbsp;
            <Text
              textType={TextTypes.Body}
              className="Legend--container__body--label-suffix"
            >
              std dev
            </Text>
          </span>
        </div>
        <div className="Legend--container__body--element">
          <Chip title={`${stddevPlus2.toFixed(2)}ms`} type="blue" />
          <span className="Legend--container__body--label">
            <Text textType={TextTypes.BodyStrong}>{`+2`}</Text>&nbsp;
            <Text
              textType={TextTypes.Body}
              className="Legend--container__body--label-suffix"
            >
              std dev
            </Text>
          </span>
        </div>
      </div>
    </div>
  </div>
);
