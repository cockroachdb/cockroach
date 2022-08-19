// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { Tooltip } from "@cockroachlabs/ui-components";
import React from "react";
import { Link } from "react-router-dom";

import jobStyles from "../jobs.module.scss";
import sortedTableStyles from "src/sortedtable/sortedtable.module.scss";
import classNames from "classnames/bind";

const jobCx = classNames.bind(jobStyles);
const tableCx = classNames.bind(sortedTableStyles);

type Job = cockroach.server.serverpb.IJobResponse;

export class JobDescriptionCell extends React.PureComponent<{ job: Job }> {
  render(): React.ReactElement {
    // If a [SQL] job.statement exists, it means that job.description
    // is a human-readable message. Otherwise job.description is a SQL
    // statement.
    const job = this.props.job;
    const additionalStyle = job.statement
      ? ""
      : jobCx(" jobs-table__cell--sql");
    const description =
      job.description && job.description.length > 425
        ? `${job.description.slice(0, 425)}...`
        : job.description;

    const cellContent = (
      <div className={jobCx("jobs-table__cell--description")}>
        {job.statement || job.description || job.type}
      </div>
    );
    return (
      <Link className={`${additionalStyle}`} to={`jobs/${String(job.id)}`}>
        <div className={tableCx("cl-table-link__tooltip")}>
          {description ? (
            <Tooltip
              placement="bottom"
              content={
                <pre
                  style={{ whiteSpace: "pre-wrap" }}
                  className={tableCx("cl-table-link__description")}
                >
                  {description}
                </pre>
              }
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
