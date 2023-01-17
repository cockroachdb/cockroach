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
  },
  refreshNodes: () => dispatch(nodesActions.refresh()),
  refreshUserSQLRoles: () => dispatch(uiConfigActions.refreshUserSQLRoles()),
});

export const ConnectedIndexDetailsPage = withRouter<any, any>(
  connect(mapStateToProps, mapDispatchToProps)(IndexDetailsPage),
);
