// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  ActiveTransactionFilters,
  ActiveTransactionsViewDispatchProps,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import { selectAppName, selectActiveTransactions } from "src/selectors";
import { refreshLiveWorkload } from "src/redux/apiReducers";
import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

const transactionsColumnsLocalSetting = new LocalSetting<
  AdminUIState,
  string | null
>(
  "columns/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  null,
);

const defaultActiveTxnFilters = { app: defaultFilters.app };

const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  ActiveTransactionFilters
>(
  "filters/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  defaultActiveTxnFilters,
);

const sortSettingLocalSetting = new LocalSetting<AdminUIState, SortSetting>(
  "sortSetting/ActiveTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

export const mapStateToActiveTransactionsPageProps = (state: AdminUIState) => ({
  selectedColumns: transactionsColumnsLocalSetting.selectorToArray(state),
  transactions: selectActiveTransactions(state),
  sessionsError: state.cachedData?.sessions.lastError,
  filters: filtersLocalSetting.selector(state),
  sortSetting: sortSettingLocalSetting.selector(state),
  internalAppNamePrefix: selectAppName(state),
});

// This object is just for convenience so we don't need to supply dispatch to
// each action.
export const activeTransactionsPageActionCreators: ActiveTransactionsViewDispatchProps =
  {
    onColumnsSelect: (columns: string[]) =>
      transactionsColumnsLocalSetting.set(columns.join(",")),
    onFiltersChange: (filters: ActiveTransactionFilters) =>
      filtersLocalSetting.set(filters),
    onSortChange: (ss: SortSetting) => sortSettingLocalSetting.set(ss),
    refreshLiveWorkload,
  };
