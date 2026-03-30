// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  Filters,
  defaultFilters,
  TransactionsPageStateProps,
  TransactionsPageDispatchProps,
  TransactionsPageRoot,
  TransactionsPageRootProps,
  api,
} from "@cockroachlabs/cluster-ui";
import { connect } from "react-redux";
import { RouteComponentProps, withRouter } from "react-router-dom";
import { bindActionCreators } from "redux";

import { trackApplySearchCriteriaAction } from "src/redux/analyticsActions";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";

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

const fingerprintsPageActions = {
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
};

type DispatchProps = {
  fingerprintsPageProps: TransactionsPageDispatchProps;
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
        timeScale: selectTimeScale(state),
        filters: filtersLocalSetting.selector(state),
        search: searchLocalSetting.selector(state),
        sortSetting: sortSettingLocalSetting.selector(state),
        limit: limitSetting.selector(state),
        reqSortSetting: reqSortSetting.selector(state),
        requestTime: requestTimeLocalSetting.selector(state),
      },
    }),
    dispatch => ({
      fingerprintsPageProps: bindActionCreators(
        fingerprintsPageActions,
        dispatch,
      ),
    }),
    (stateProps, dispatchProps) => ({
      fingerprintsPageProps: {
        ...stateProps.fingerprintsPageProps,
        ...dispatchProps.fingerprintsPageProps,
      },
    }),
  )(TransactionsPageRoot),
);

export default TransactionsPageConnected;
