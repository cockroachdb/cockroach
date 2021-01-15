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
import {
  clusterStore,
  nodeLivenessIssues,
  clusterGlossary,
  reviewOfCockroachTerminology,
  howAreCapacityMetricsCalculatedOverview,
} from "src/util/docs";
import { TooltipProps } from "src/components/tooltip/tooltip";

export const CapacityUsageTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Usage of disk space by CockroachDB data.</p>
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

export const UsedTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Total disk space in use by CockroachDB data across all nodes.</p>
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

export const UsableTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Total disk space usable by CockroachDB data across all nodes.</p>
        <p>
          {"This cannot exceed the store size, if one has been set using "}
          <Anchor href={clusterStore} target="_blank">
            --store
          </Anchor>
          .
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const LiveNodesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Node is online and responding.</p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const SuspectNodesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Node is online and responding.</p>
        <p>
          {"Node has an "}
          <Anchor href={nodeLivenessIssues} target="_blank">
            unavailable liveness status
          </Anchor>
          .
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const DeadNodesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          Node has not responded for 5 minutes. CockroachDB automatically
          rebalances replicas from dead nodes to live nodes.
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const TotalRangesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          {"Total number of "}
          <Anchor href={clusterGlossary} target="_blank">
            ranges
          </Anchor>
          {" in the cluster."}
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const UnderReplicatedRangesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          {"Number of "}
          <Anchor href={reviewOfCockroachTerminology} target="_blank">
            under-replicated ranges
          </Anchor>
          {" in the cluster. A non-zero number indicates an unstable cluster."}
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);

export const UnavailableRangesTooltip: React.FC<TooltipProps> = (props) => (
  <Tooltip
    {...props}
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          {"Number of "}
          <Anchor href={reviewOfCockroachTerminology} target="_blank">
            unavailable ranges
          </Anchor>
          {" in the cluster. A non-zero number indicates an unstable cluster."}
        </p>
      </div>
    }
  >
    {props.children}
  </Tooltip>
);
