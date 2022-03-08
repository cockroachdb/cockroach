// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { withRouter } from "react-router-dom";
import { connect } from "react-redux";
import { Dispatch } from "redux";
import { actions as sessionsActions } from "src/store/sessions";
import { actions as localStorageActions } from "src/store/localStorage";
import { AppState } from "../store";
import {
  selectActiveTransactions,
  selectColumns,
  selectFilters,
  selectSortSetting,
} from "./activeTransactionsPage.selectors";
import {
  ActiveTransactionsView,
  ActiveTransactionsViewDispatchProps,
  ActiveTransactionsViewStateProps,
} from ".";
import { ActiveTransactionFilters, SortSetting } from "src";

// For tenant cases, we don't show information about node, regions and
// diagnostics.
const mapStateToProps = (state: AppState): ActiveTransactionsViewStateProps => {
  return {
    transactions: selectActiveTransactions(state),
    fetchError: state.adminUI.sessions.lastError,
    selectedColumns: selectColumns(state),
    sortSetting: selectSortSetting(state),
    filters: selectFilters(state),
  };
};

const mapDispatchToProps = (
  dispatch: Dispatch,
): ActiveTransactionsViewDispatchProps => ({
  refreshSessions: () => dispatch(sessionsActions.refresh()),
  onColumnsSelect: columns =>
    dispatch(
      localStorageActions.update({
        key: "showColumns/ActiveTransactionsPage",
        value: columns ? columns.join(", ") : " ",
      }),
    ),
  onFiltersChange: (filters: ActiveTransactionFilters) =>
    dispatch(
      localStorageActions.update({
        key: "filters/ActiveTransactionsPage",
        value: filters,
      }),
    ),
  onSortChange: (ss: SortSetting) =>
    dispatch(
      localStorageActions.update({
        key: "sortSetting/ActiveTransactionsPage",
        value: ss,
      }),
    ),
});

export const ConnectedActiveTransactionsPage = withRouter(
  connect(mapStateToProps, mapDispatchToProps)(ActiveTransactionsView),
);
