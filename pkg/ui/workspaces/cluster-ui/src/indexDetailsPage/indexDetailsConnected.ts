// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AppState, uiConfigActions } from "../store";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { selectIndexDetails } from "./indexDetails.selectors";
import { Dispatch } from "redux";
import { IndexDetailPageActions, IndexDetailsPage } from "./indexDetailsPage";
import { connect } from "react-redux";
import { actions as indexStatsActions } from "src/store/indexStats/indexStats.reducer";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { actions as nodesActions } from "../store/nodes";
import { TimeScale } from "../timeScaleDropdown";
import { actions as sqlStatsActions } from "../store/sqlStats";
import { actions as analyticsActions } from "../store/analytics";

const mapStateToProps = (state: AppState, props: RouteComponentProps) => {
  return selectIndexDetails(state, props);
};

const mapDispatchToProps = (dispatch: Dispatch): IndexDetailPageActions => ({
  refreshIndexStats: (database: string, table: string) => {
    dispatch(
      indexStatsActions.refresh(
        new cockroach.server.serverpb.TableIndexStatsRequest({
          database,
          table,
        }),
      ),
    );
  },
  resetIndexUsageStats: (database: string, table: string) => {
    dispatch(
      indexStatsActions.reset({
        database,
        table,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "Reset Index Usage",
        page: "Index Details",
      }),
    );
  },
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
  onTimeScaleChange: (ts: TimeScale) => {
    dispatch(
      sqlStatsActions.updateTimeScale({
        ts: ts,
      }),
    );
    dispatch(
      analyticsActions.track({
        name: "TimeScale changed",
        page: "Index Details",
        value: ts.key,
      }),
    );
  },
});

export const ConnectedIndexDetailsPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(IndexDetailsPage),
);
