// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Tooltip, Anchor } from "src/components";
import { howAreCapacityMetricsCalculatedOverview, clusterStore } from "src/util/docs";
import { TooltipProps } from "src/components/tooltip/tooltip";

export const NodeArcPercentageTooltip: React.FC<TooltipProps & { localityLabel?: string; nodeLabel?: string }> = (props) => (
    <Tooltip
        {...props}
        placement="bottom"
        title={
            <div className="tooltip__table--title">
                <p>
                    Percentage of usable disk space occupied by CockroachDB data &nbsp;
                    {props.localityLabel && `at ${props.localityLabel}.`}
                    {props.nodeLabel && `on node ${props.nodeLabel}.`}
                    {!props.nodeLabel && !props.localityLabel && "at <locality> / on node <node>"}
                </p>
                <p>
                    <Anchor href={howAreCapacityMetricsCalculatedOverview} target="_blank">
                        How are these metrics calculated?
                    </Anchor>
                </p>
            </div>
        }
    >
        {props.children}
    </Tooltip>
);

export const NodeArcUsedCapacityTooltip: React.FC<TooltipProps & { localityLabel?: string; nodeLabel?: string }> = (props) => (
    <Tooltip
        {...props}
        placement="bottom"
        title={
            <div className="tooltip__table--title">
                <p>
                    Disk space in use by CockroachDB data &nbsp;
                    {props.localityLabel && `at ${props.localityLabel}.`}
                    {props.nodeLabel && `on node ${props.nodeLabel}.`}
                    {!props.nodeLabel && !props.localityLabel && "at <locality> / on node <node>"}
                </p>
                <p>
                    This excludes the Cockroach binary, operating system, and other system files.
                </p>
            </div>
        }
    >
        {props.children}
    </Tooltip>
);

export const NodeArcTotalCapacityTooltip: React.FC<TooltipProps & { localityLabel?: string; nodeLabel?: string }> = (props) => (
    <Tooltip
        {...props}
        placement="bottom"
        title={
            <div className="tooltip__table--title">
                <p>
                    Total disk space usable by CockroachDB data &nbsp;
                    {props.localityLabel && `at ${props.localityLabel}.`}
                    {props.nodeLabel && `on node ${props.nodeLabel}.`}
                    {!props.nodeLabel && !props.localityLabel && "at <locality> / on node <node>"}
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
