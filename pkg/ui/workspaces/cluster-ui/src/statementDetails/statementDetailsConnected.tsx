// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React, { useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useHistory, useLocation, useRouteMatch } from "react-router-dom";

import { StatementDiagnosticsReport } from "src/api";
import { selectRequestTime } from "src/statementsPage/statementsPage.selectors";
import { actions as analyticsActions } from "src/store/analytics";
import { actions as localStorageActions } from "src/store/localStorage";
import { getMatchParamByName, statementAttr } from "src/util";

import { selectTimeScale } from "../store/utils/selectors";
import { TimeScale } from "../timeScaleDropdown";

import { StatementDetails } from "./statementDetails";

export const ConnectedStatementDetailsPage: React.FC = () => {
  const history = useHistory();
  const location = useLocation();
  const match = useRouteMatch<{ statement: string }>();
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const requestTime = useSelector(selectRequestTime);
  const statementFingerprintID = getMatchParamByName(match, statementAttr);

  const onTimeScaleChange = useCallback(
    (ts: TimeScale) => {
      dispatch(localStorageActions.updateTimeScale({ value: ts }));
      dispatch(
        analyticsActions.track({
          name: "TimeScale changed",
          page: "Statement Details",
          value: ts.key,
        }),
      );
    },
    [dispatch],
  );

  const dismissStatementDiagnosticsAlertMessage = useCallback(() => {
    dispatch(
      localStorageActions.update({
        key: "adminUi/showDiagnosticsModal",
        value: false,
      }),
    );
  }, [dispatch]);

  const onTabChanged = useCallback(
    (tabName: string) => {
      dispatch(
        analyticsActions.track({
          name: "Tab Changed",
          page: "Statement Details",
          tabName,
        }),
      );
    },
    [dispatch],
  );

  const onDiagnosticBundleDownload = useCallback(() => {
    dispatch(
      analyticsActions.track({
        name: "Statement Diagnostics Clicked",
        page: "Statement Details",
        action: "Downloaded",
      }),
    );
  }, [dispatch]);

  const onActivateStatementDiagnosticsAnalytics = useCallback(() => {
    dispatch(
      analyticsActions.track({
        name: "Statement Diagnostics Clicked",
        page: "Statement Details",
        action: "Activated",
      }),
    );
  }, [dispatch]);

  const onDiagnosticCancelRequestTracking = useCallback(
    (_report: StatementDiagnosticsReport) => {
      dispatch(
        analyticsActions.track({
          name: "Statement Diagnostics Clicked",
          page: "Statement Details",
          action: "Cancelled",
        }),
      );
    },
    [dispatch],
  );

  const onSortingChange = useCallback(
    (tableName: string, columnName: string) => {
      dispatch(
        analyticsActions.track({
          name: "Column Sorted",
          page: "Statement Details",
          columnName,
          tableName,
        }),
      );
    },
    [dispatch],
  );

  const onRequestTimeChange = useCallback(
    (t: moment.Moment) => {
      dispatch(
        localStorageActions.update({
          key: "requestTime/StatementsPage",
          value: t,
        }),
      );
    },
    [dispatch],
  );

  const onBackToStatementsClick = useCallback(() => {
    dispatch(
      analyticsActions.track({
        name: "Back Clicked",
        page: "Statement Details",
      }),
    );
  }, [dispatch]);

  return (
    <StatementDetails
      history={history}
      location={location}
      match={match}
      statementFingerprintID={statementFingerprintID}
      timeScale={timeScale}
      requestTime={requestTime}
      dismissStatementDiagnosticsAlertMessage={
        dismissStatementDiagnosticsAlertMessage
      }
      onTabChanged={onTabChanged}
      onTimeScaleChange={onTimeScaleChange}
      onDiagnosticBundleDownload={onDiagnosticBundleDownload}
      onActivateStatementDiagnosticsAnalytics={
        onActivateStatementDiagnosticsAnalytics
      }
      onDiagnosticCancelRequestTracking={onDiagnosticCancelRequestTracking}
      onSortingChange={onSortingChange}
      onRequestTimeChange={onRequestTimeChange}
      onBackToStatementsClick={onBackToStatementsClick}
    />
  );
};
