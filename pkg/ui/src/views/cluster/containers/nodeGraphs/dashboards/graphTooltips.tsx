// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import * as docsURL from "src/util/docs";
import {Anchor} from "src/components";
import {ReactNode} from "react";

export const CapacityGraphTooltip: React.FunctionComponent<{tooltipSelection: ReactNode}> =
  ({tooltipSelection}) => (<div>
    <dl>
      <dd>
        <p>
          {`Usage of disk space across all ${tooltipSelection} / on node <node>.`}
        </p>
        <p>
          <Anchor href={docsURL.howAreCapacityMetricsCalculated}>
            How are these metrics calculated?
          </Anchor>
        </p>
      </dd>
    </dl>
  </div>);

export const LogicalBytesGraphTooltip: React.FunctionComponent =
  () => (<div>
    <dl>
      <dd>
        <p>
          {"Number of logical bytes stored in "}
          <Anchor href={docsURL.keyValuePairs}>
            key-value pairs
          </Anchor>
          {" on each node."}
        </p>
        <p>
          This includes historical and deleted data.
        </p>
      </dd>
    </dl>
  </div>);
