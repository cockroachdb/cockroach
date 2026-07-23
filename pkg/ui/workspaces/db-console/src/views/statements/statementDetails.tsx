// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { StatementDetails, TimeScale } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import React, { useCallback } from "react";
import { useDispatch, useSelector } from "react-redux";
import { useHistory, useLocation, useRouteMatch } from "react-router-dom";

import { createStatementDiagnosticsAlertLocalSetting } from "src/redux/alerts";
import { AdminUIState } from "src/redux/state";
import { setGlobalTimeScaleAction } from "src/redux/statements";
import { selectTimeScale } from "src/redux/timeScale";
import {
  trackActivateDiagnostics,
  trackDownloadDiagnosticsBundle,
  trackSubnavSelection,
} from "src/util/analytics";
import trackCancelDiagnosticsBundle from "src/util/analytics/trackCancelDiagnosticsBundle";
import { statementAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";

import { requestTimeLocalSetting } from "./statementsPage";

const ConnectedStatementDetailsPage: React.FC = () => {
  const history = useHistory();
  const location = useLocation();
  const match = useRouteMatch<{ statement: string }>();
  const dispatch = useDispatch();
  const timeScale = useSelector(selectTimeScale);
  const requestTime = useSelector((state: AdminUIState) =>
    requestTimeLocalSetting.selector(state),
  );
  const statementFingerprintID = getMatchParamByName(match, statementAttr);

  const dismissStatementDiagnosticsAlertMessage = useCallback(() => {
    dispatch(createStatementDiagnosticsAlertLocalSetting.set({ show: false }));
  }, [dispatch]);

  const onTimeScaleChange = useCallback(
    (ts: TimeScale) => {
      dispatch(setGlobalTimeScaleAction(ts));
    },
    [dispatch],
  );

  const onRequestTimeChange = useCallback(
    (t: moment.Moment) => {
      dispatch(requestTimeLocalSetting.set(t));
    },
    [dispatch],
  );

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
      onTabChanged={trackSubnavSelection}
      onTimeScaleChange={onTimeScaleChange}
      onRequestTimeChange={onRequestTimeChange}
      onDiagnosticBundleDownload={trackDownloadDiagnosticsBundle}
      onActivateStatementDiagnosticsAnalytics={trackActivateDiagnostics}
      onDiagnosticCancelRequestTracking={report =>
        trackCancelDiagnosticsBundle(report.statement_fingerprint)
      }
    />
  );
};

export default ConnectedStatementDetailsPage;
