// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";

import { Badge, Link, Tooltip } from "src/components";
import { DiagnosticStatuses } from "./diagnosticStatuses";
import "./diagnosticStatusBadge.styl";

interface OwnProps {
  status: DiagnosticStatuses;
  enableTooltip?: boolean;
}

function mapDiagnosticsStatusToBadge(diagnosticsStatus: DiagnosticStatuses) {
  switch (diagnosticsStatus) {
    case "READY":
      return "success";
    case "WAITING FOR QUERY":
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
        <div>
          {`Go to the detail page for this statement and click the ‘diagnostics’ tab.
           The statement diagnostics download will include EXPLAIN plans, table statistics, and traces. `}
          <Link
            href="" // TODO (koorosh): Provide meaningful url on docs.
            target="_blank"
          >
            Learn more
          </Link>
        </div>
      );
    case "WAITING FOR QUERY":
      return (
        <div>
          <span>
            CockroachDB is waiting for the next query that matches this statement fingerprint.
          </span>
          <p/>
          <span>
            {`A download button will appear on the statement list and detail pages when the query is ready.
            The download will include EXPLAIN plans, table statistics, and traces. `}
            <Link
              href="" // TODO (koorosh): Provide meaningful url on docs.
              target="_blank"
            >
              Learn more
            </Link>
          </span>
        </div>
      );
    case "ERROR":
      return (
        <div>
          {"There was an error when attempting to collect diagnostics for this statement. Please try activating again. "}
          <Link
            href="" // TODO (koorosh): Provide meaningful url on docs.
            target="_blank"
          >
            Learn more
          </Link>
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
    return (
      <Badge
        text={status}
        status={mapDiagnosticsStatusToBadge(status)}
      />
    );
  }

  return (
    <Tooltip
      visible={enableTooltip}
      title={tooltipContent}
      theme="blue"
      placement="bottom"
    >
      <div
        className="diagnostic-status-badge__content"
      >
        <Badge
          text={status}
          status={mapDiagnosticsStatusToBadge(status)}
        />
      </div>
    </Tooltip>
  );
}

DiagnosticStatusBadge.defaultProps = {
  enableTooltip: true,
};
