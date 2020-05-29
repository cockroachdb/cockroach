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
} from "src/util/docs";

export const UsedCapacityTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
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
    {children}
  </Tooltip>
);

export const UsableCapacityTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
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
    {children}
  </Tooltip>
);

export const LiveNodesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>Node is online and responding.</p>
      </div>
    }
  >
    {children}
  </Tooltip>
);

export const SuspectNodesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
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
    {children}
  </Tooltip>
);

export const DeadNodesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
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
    {children}
  </Tooltip>
);

export const TotalRangesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
  children,
}) => (
  <Tooltip
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
    {children}
  </Tooltip>
);

export const UnderReplicatedRangesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
    children,
  }) => (
    <Tooltip
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          {"Number of "}
          <Anchor
            href={reviewOfCockroachTerminology}
            target="_blank"
          >
            under-replicated ranges
          </Anchor>
          {" in the cluster. A non-zero number indicates an unstable cluster."}
        </p>
      </div>
    }
  >
      {children}
    </Tooltip>
  );

export const  UnavailableRangesTooltip: React.FC<React.PropsWithChildren<{}>> = ({
    children,
  }) => (
    <Tooltip
    placement="bottom"
    title={
      <div className="tooltip__table--title">
        <p>
          {"Number of "}
          <Anchor
            href={reviewOfCockroachTerminology}
            target="_blank"
          >
            unavailable ranges
          </Anchor>
          {" in the cluster. A non-zero number indicates an unstable cluster."}
        </p>
      </div>
    }
  >
      {children}
    </Tooltip>
  );
