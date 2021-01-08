// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";
import React from "react";

import * as protos from "src/js/protos";

import { AdminUIState } from "src/redux/state";
import {
  refreshDatabaseDetails,
  refreshTableDetails,
  refreshTableStats,
  generateTableID,
  KeyedCachedDataReducerState,
} from "src/redux/apiReducers";

import { SortSetting } from "src/views/shared/components/sortabletable";

import { TableInfo } from "src/views/databases/data/tableInfo";

// DatabaseSummaryImplicitData describes properties which must be explicitly set
// on a DatabaseSummary component.
export interface DatabaseSummaryExplicitData {
  name: string;
  updateOnLoad?: boolean;
}

// DatabaseSummaryConnectedData describes properties which are applied to a
// DatabaseSummary component by connecting to a redux store.
interface DatabaseSummaryConnectedData {
  sortSetting: SortSetting;
  tableInfos: TableInfo[];
  dbResponse: KeyedCachedDataReducerState<protos.cockroach.server.serverpb.DatabaseDetailsResponse>;
  grants: protos.cockroach.server.serverpb.DatabaseDetailsResponse.Grant[];
}

// DatabaseSummaryActions describes actions which can be dispatched by a
// DatabaseSummary component.
interface DatabaseSummaryActions {
  setSort: (setting: SortSetting) => void;
  refreshDatabaseDetails: typeof refreshDatabaseDetails;
  refreshTableDetails: typeof refreshTableDetails;
  refreshTableStats: typeof refreshTableStats;
}

export type DatabaseSummaryProps = DatabaseSummaryExplicitData &
  DatabaseSummaryConnectedData &
  DatabaseSummaryActions;

interface DatabaseSummaryState {
  finishedLoadingTableData: boolean;
}

// DatabaseSummaryBase implements common lifecycle methods for DatabaseSummary
// components, which differ primarily by their render() method.
// TODO(mrtracy): We need to find a better abstraction for the common
// "refresh-on-mount-or-receiveProps" we have in many of our connected
// components; that would allow us to avoid this inheritance.
export class DatabaseSummaryBase extends React.Component<
  DatabaseSummaryProps,
  DatabaseSummaryState
> {
  // loadTableDetails loads data for each table which have no info in the store.
  // TODO(mrtracy): Should this be refreshing data always? Not sure if there
  // is a performance concern with invalidation periods.
  async loadTableDetails(props = this.props) {
    if (props.tableInfos && props.tableInfos.length > 0) {
      for (const tblInfo of props.tableInfos) {
        // TODO(davidh): this is a stopgap inserted to deal with DBs containing hundreds of tables
        await Promise.all([
          _.isUndefined(tblInfo.numColumns)
            ? props.refreshTableDetails(
                new protos.cockroach.server.serverpb.TableDetailsRequest({
                  database: props.name,
                  table: tblInfo.name,
                }),
              )
            : null,
          _.isUndefined(tblInfo.physicalSize)
            ? props.refreshTableStats(
                new protos.cockroach.server.serverpb.TableStatsRequest({
                  database: props.name,
                  table: tblInfo.name,
                }),
              )
            : null,
        ]);
      }
    }
    this.setState({ finishedLoadingTableData: true });
  }

  // Refresh when the component is mounted.
  async componentDidMount() {
    this.props.refreshDatabaseDetails(
      new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
        database: this.props.name,
      }),
    );
    if (this.props.updateOnLoad) {
      await this.loadTableDetails();
    }
  }

  // Refresh when the component receives properties.
  async componentDidUpdate() {
    if (this.props.updateOnLoad) {
      await this.loadTableDetails(this.props);
    }
  }

  // Leaving this render method alone during linting cleanup since it's
  // used to discourage render without subclassing.
  // eslint-disable-next-line react/require-render-return
  render(): React.ReactElement<any> {
    throw new Error(
      "DatabaseSummaryBase should never be instantiated directly. ",
    );
  }
}

export function databaseDetails(state: AdminUIState) {
  return state.cachedData.databaseDetails;
}

// Function which returns TableInfo objects for all tables in a database. This
// is not a selector, because it is not based only on the Redux state - it is
// also based on the tables in a single database.
// TODO(mrtracy): look into using a component like reselect-map if this proves
// to be expensive. My current intuition is that this will not be a bottleneck.
export function tableInfos(state: AdminUIState, dbName: string) {
  const dbDetails = databaseDetails(state);
  const tableNames =
    dbDetails[dbName] &&
    dbDetails[dbName].data &&
    dbDetails[dbName].data.table_names;
  if (!tableNames) {
    return null;
  }
  const details = state.cachedData.tableDetails;
  const stats = state.cachedData.tableStats;
  return _.map(tableNames, (tableName) => {
    const tblId = generateTableID(dbName, tableName);
    const tblDetails = details[tblId] && details[tblId].data;
    const tblStats = stats[tblId] && stats[tblId].data;
    return new TableInfo(tableName, tblDetails, tblStats);
  });
}

// Function which extracts the grants for a single database from redux state.
export function grants(state: AdminUIState, dbName: string) {
  const dbDetails = databaseDetails(state);
  return (
    dbDetails[dbName] && dbDetails[dbName].data && dbDetails[dbName].data.grants
  );
}
