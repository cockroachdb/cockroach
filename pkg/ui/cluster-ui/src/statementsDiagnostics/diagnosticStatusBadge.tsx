// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import classNames from "classnames/bind";

import { Badge } from "src/badge";
import { Anchor } from "src/anchor";
import { Tooltip } from "@cockroachlabs/ui-components";
import { statementDiagnostics } from "src/util";
import { DiagnosticStatuses } from "./diagnosticStatuses";
import styles from "./diagnosticStatusBadge.module.scss";

interface OwnProps {
  status: DiagnosticStatuses;
  enableTooltip?: boolean;
}

const cx = classNames.bind(styles);

function mapDiagnosticsStatusToBadge(diagnosticsStatus: DiagnosticStatuses) {
  switch (diagnosticsStatus) {
    case "READY":
      return "success";
    case "WAITING":
      return "info";
    case "ERROR":
      return "danger";
    default:
      return "info";
  }
}

function mapStatusToDescription(diagnosticsStatus: DiagnosticStatuses) {
  switch (diagnosticsStatus) {
    case "READY":
      return (
        <div className={cx("tooltip__table--title")}>
          <p>
            {"The most recent "}
            <Anchor href={statementDiagnostics} target="_blank">
              diagnostics
            </Anchor>
            {
              " for this SQL statement fingerprint are ready to download. Access the full history of diagnostics for the fingerprint in the Statement Details page."
            }
          </p>
        </div>
      );
    case "WAITING":
      return (
        <div className={cx("tooltip__table--title")}>
          <p>
            CockroachDB is waiting for the next SQL statement that matches this
            fingerprint.
          </p>
          <p>
            {"When the most recent "}
            <Anchor href={statementDiagnostics} target="_blank">
              diagnostics
            </Anchor>
            {" are ready to download, a link will appear in this row."}
          </p>
        </div>
      );
    case "ERROR":
      return (
        <div>
          {
            "There was an error when attempting to collect diagnostics for this statement. Please try activating again. "
          }
          <Anchor href={statementDiagnostics} target="_blank">
            Learn more
          </Anchor>
        </div>
      );
    default:
      return "";
  }
}

export function DiagnosticStatusBadge(props: OwnProps) {
  const { status, enableTooltip } = props;
  const tooltipContent = mapStatusToDescription(status);

  if (!enableTooltip) {
    return <Badge text={status} status={mapDiagnosticsStatusToBadge(status)} />;
  }

  return (
    <Tooltip content={tooltipContent} style="default" placement="left-end">
      <div className={cx("diagnostic-status-badge__content")}>
        <Badge text={status} status={mapDiagnosticsStatusToBadge(status)} />
      </div>
    </Tooltip>
  );
}

DiagnosticStatusBadge.defaultProps = {
  enableTooltip: true,
};
