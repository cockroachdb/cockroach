import React from "react";
import {Link} from "react-router-dom";
import {Tooltip} from "src/components";
import Job = cockroach.server.serverpb.JobsResponse.IJob;
import {cockroach} from "src/js/protos";

export class JobDescriptionCell extends React.PureComponent<{ job: Job }> {
  render() {
    // If a [SQL] job.statement exists, it means that job.description
    // is a human-readable message. Otherwise job.description is a SQL
    // statement.
    const job = this.props.job;
    const additionalStyle = (job.statement ? "" : " jobs-table__cell--sql");
    return (
      <Link className={`jobs-table__cell--description${additionalStyle}`} to={`jobs/${String(job.id)}`}>
        <div className="cl-table-link__tooltip">
          <Tooltip overlayClassName="preset-black" placement="bottom" title={
            <pre style={{whiteSpace: "pre-wrap"}}>{job.description}</pre>
          }>
            <div className={`jobs-table__cell--description${additionalStyle}`}>
              {job.statement || job.description}
            </div>
          </Tooltip>
        </div>
      </Link>
    );
  }
}
