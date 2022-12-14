// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useEffect } from "react";
import { Visualization } from "../graphs";
import { Loading } from "../loading";
import { ContentionEventsResponse } from "../api/txnContentionApi";
import { ContentionGraph } from "./contentionGraph";

const GRAPH_TITLE = "Transaction Contention"
const GRAPH_TOOLTIP = "This graph displays the network of tranasction contention across your cluster."

export type ContentionDebugStateProps = {
  contentionEvents: ContentionEventsResponse;
  contentionError: Error | null;
};

export type ContentionDebugDispatchProps = {
  refreshTxnContentionEvents: () => void;
};

export type ContentionDebugPageProps = ContentionDebugStateProps &
  ContentionDebugDispatchProps;

export const ContentionDebugPage: React.FC<ContentionDebugPageProps> = (
  props: ContentionDebugPageProps,
) => {
  const {
    contentionEvents,
    contentionError,
    refreshTxnContentionEvents
  } = props;

  useEffect(() => {
    // Refresh every 10 seconds.
    refreshTxnContentionEvents();
    const interval = setInterval(refreshTxnContentionEvents, 10 * 1000);
    return () => {
      clearInterval(interval);
    };
  }, [refreshTxnContentionEvents]);

  return (
    <div>
        <Loading
          loading={contentionEvents === null}
          page="statement insights"
          error={contentionError}
        >
          <Visualization
            title={GRAPH_TITLE}
            loading={contentionEvents === null}
            tooltip={GRAPH_TOOLTIP}
          >
            <ContentionGraph contentionEvents={contentionEvents}/>
          </Visualization>
        </Loading>
    </div>
  );
};
