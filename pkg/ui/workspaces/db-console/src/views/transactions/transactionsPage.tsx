// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Filters,
  defaultFilters,
  util,
  TransactionsPageStateProps,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
  TransactionsPageDispatchProps,
  TransactionsPageRoot,
  TransactionsPageRootProps,
  api,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { bindActionCreators } from "redux";
import { createSelector } from "reselect";

import { trackApplySearchCriteriaAction } from "src/redux/analyticsActions";
import {
  createSelectorForCachedDataField,
  refreshNodes,
  refreshTxns,
  refreshUserSQLRoles,
} from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { nodeRegionsByIDSelector } from "src/redux/nodes";
import { resetSQLStatsAction } from "src/redux/sqlStats";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import { selectHasAdminRole } from "src/redux/user";
import { PrintTime } from "src/views/reports/containers/range/print";

import {
  activeTransactionsPageActionCreators,
  mapStateToActiveTransactionsPageProps,
} from "./activeTransactionsSelectors";

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(
  (state: AdminUIState) => state.cachedData.transactions,
  state => {
    if (!state?.data) {
      return "unknown";
    }

    return PrintTime(util.TimestampToMoment(state.data.last_reset));
  },
);

const selectOldestDate = createSelector(
  (state: AdminUIState) => state.cachedData.transactions,
  txns => {
    return txns?.data?.oldest_aggregated_ts_returned;
  },
);

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "executionCount" },
);

export const requestTimeLocalSetting = new LocalSetting(
  "requestTime/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const filtersLocalSetting = new LocalSetting<AdminUIState, Filters>(
  "filters/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultFilters,
);

export const searchLocalSetting = new LocalSetting(
  "search/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const transactionColumnsLocalSetting = new LocalSetting(
  "showColumns/TransactionPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

export const reqSortSetting = new LocalSetting(
  "reqSortSetting/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.sortTxn,
);

export const limitSetting = new LocalSetting(
  "reqLimitSetting/TransactionsPage",
  (state: AdminUIState) => state.localSettings,
  api.DEFAULT_STATS_REQ_OPTIONS.limit,
);

export const selectTxns =
  createSelectorForCachedDataField<api.SqlStatsResponse>("transactions");

const fingerprintsPageActions = {
  refreshData: refreshTxns,
  refreshNodes,
  refreshUserSQLRoles,
  resetSQLStats: resetSQLStatsAction,
  onTimeScaleChange: setGlobalTimeScaleAction,
  // We use `null` when the value was never set and it will show all columns.
  // If the user modifies the selection and no columns are selected,
  // the function will save the value as a blank space, otherwise
  // it gets saved as `null`.
  onColumnsChange: (value: string[]) =>
    transactionColumnsLocalSetting.set(
      value.length === 0 ? " " : value.join(","),
    ),
  onSortingChange: (
    _tableName: string,
    columnName: string,
    ascending: boolean,
  ) =>
    sortSettingLocalSetting.set({
      ascending: ascending,
      columnTitle: columnName,
    }),
  onFilterChange: (filters: Filters) => filtersLocalSetting.set(filters),
  onSearchComplete: (query: string) => searchLocalSetting.set(query),
  onChangeLimit: (newLimit: number) => limitSetting.set(newLimit),
  onChangeReqSort: (sort: api.SqlStatsSortType) => reqSortSetting.set(sort),
  onApplySearchCriteria: trackApplySearchCriteriaAction,
  onRequestTimeChange: (t: moment.Moment) => requestTimeLocalSetting.set(t),
};

type StateProps = {
  fingerprintsPageProps: TransactionsPageStateProps & RouteComponentProps;
  activePageProps: ActiveTransactionsViewStateProps;
};

type DispatchProps = {
  fingerprintsPageProps: TransactionsPageDispatchProps;
  activePageProps: ActiveTransactionsViewDispatchProps;
};

const TransactionsPageConnected = withRouter(
  connect<
    StateProps,
    DispatchProps,
    RouteComponentProps,
    TransactionsPageRootProps,
    AdminUIState
  >(
    (state: AdminUIState, props: RouteComponentProps) => ({
      fingerprintsPageProps: {
        ...props,
        columns: transactionColumnsLocalSetting.selectorToArray(state),
        txnsResp: selectTxns(state),
        timeScale: selectTimeScale(state),
        filters: filtersLocalSetting.selector(state),
        lastReset: selectLastReset(state),
        nodeRegions: nodeRegionsByIDSelector(state),
        search: searchLocalSetting.selector(state),
        sortSetting: sortSettingLocalSetting.selector(state),
        hasAdminRole: selectHasAdminRole(state),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
        oldestDataAvailable: selectOldestDate(state),
      },
      activePageProps: mapStateToActiveTransactionsPageProps(state),
    }),
    dispatch => ({
      fingerprintsPageProps: bindActionCreators(
        fingerprintsPageActions,
        dispatch,
      ),
      activePageProps: bindActionCreators(
        activeTransactionsPageActionCreators,
        dispatch,
      ),
    }),
    (stateProps, dispatchProps) => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
      activePageProps: {
        ...stateProps.activePageProps,
        ...dispatchProps.activePageProps,
      },
    }),
  )(TransactionsPageRoot),
);

export default TransactionsPageConnected;
