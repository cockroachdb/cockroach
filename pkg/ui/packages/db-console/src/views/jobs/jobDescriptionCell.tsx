// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Link } from "react-router-dom";
import { Tooltip } from "src/components";
import Job = cockroach.server.serverpb.IJobResponse;
import { cockroach } from "src/js/protos";

export class JobDescriptionCell extends React.PureComponent<{ job: Job }> {
  render() {
    // If a [SQL] job.statement exists, it means that job.description
    // is a human-readable message. Otherwise job.description is a SQL
    // statement.
    const job = this.props.job;
    const additionalStyle = job.statement ? "" : " jobs-table__cell--sql";
    const description =
      job.description && job.description.length > 425
        ? `${job.description.slice(0, 425)}...`
        : job.description;

    const cellContent = (
      <div className="jobs-table__cell--description">
        {job.statement || job.description || job.type}
      </div>
    );
    return (
      <Link className={`${additionalStyle}`} to={`jobs/${String(job.id)}`}>
        <div className="cl-table-link__tooltip">
          {description ? (
            <Tooltip
              arrowPointAtCenter
              placement="bottom"
              title={
                <pre
                  style={{ whiteSpace: "pre-wrap" }}
                  className="cl-table-link__description"
                >
                  {description}
                </pre>
              }
              overlayClassName="cl-table-link__statement-tooltip--fixed-width"
            >
              {cellContent}
            </Tooltip>
          ) : (
            cellContent
          )}
        </div>
      </Link>
    );
  }
}
