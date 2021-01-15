// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { Tooltip, Anchor } from "src/components";
import {
  howAreCapacityMetricsCalculatedOverview,
  clusterStore,
} from "src/util/docs";
import { TooltipProps } from "src/components/tooltip/tooltip";

export const NodeArcPercentageTooltip: React.FC<
  TooltipProps & { localityLabel?: string; nodeLabel?: string }
> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          Percentage of usable disk space occupied by CockroachDB data &nbsp;
          {props.localityLabel && `at ${props.localityLabel}.`}
          {props.nodeLabel && `on node ${props.nodeLabel}.`}
          {!props.nodeLabel &&
            !props.localityLabel &&
            "at <locality> / on node <node>"}
        </p>
        <p>
          <Anchor
            href={howAreCapacityMetricsCalculatedOverview}
            target="_blank"
          >
            How are these metrics calculated?
          </Anchor>
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const NodeArcUsedCapacityTooltip: React.FC<
  TooltipProps & { localityLabel?: string; nodeLabel?: string }
> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          Disk space in use by CockroachDB data &nbsp;
          {props.localityLabel && `at ${props.localityLabel}.`}
          {props.nodeLabel && `on node ${props.nodeLabel}.`}
          {!props.nodeLabel &&
            !props.localityLabel &&
            "at <locality> / on node <node>"}
        </p>
        <p>
          This excludes the Cockroach binary, operating system, and other system
          files.
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const NodeArcTotalCapacityTooltip: React.FC<
  TooltipProps & { localityLabel?: string; nodeLabel?: string }
> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          Total disk space usable by CockroachDB data &nbsp;
          {props.localityLabel && `at ${props.localityLabel}.`}
          {props.nodeLabel && `on node ${props.nodeLabel}.`}
          {!props.nodeLabel &&
            !props.localityLabel &&
            "at <locality> / on node <node>"}
        </p>
        <p>
          This cannot exceed the store size, if one has been set using &nbsp;
          <Anchor href={clusterStore} target="_blank">
            --store
          </Anchor>
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);
