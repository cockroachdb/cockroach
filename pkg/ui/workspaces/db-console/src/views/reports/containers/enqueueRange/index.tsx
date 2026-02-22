// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React, { Fragment, useState } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { enqueueRange } from "src/util/api";
import Print from "src/views/reports/containers/range/print";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

import "./index.scss";

import EnqueueRangeRequest = cockroach.server.serverpb.EnqueueRangeRequest;
import EnqueueRangeResponse = cockroach.server.serverpb.EnqueueRangeResponse;

const QUEUES = [
  "replicate",
  "mvccGC",
  "merge",
  "split",
  "lease",
  "replicaGC",
  "raftlog",
  "raftsnapshot",
  "consistencyChecker",
  "timeSeriesMaintenance",
];

const queueOptions = QUEUES.map(q => {
  return { value: q, label: q };
});

export type EnqueueRangeAllProps = RouteComponentProps;

function renderNodeResponse(details: EnqueueRangeResponse.IDetails) {
  return (
    <Fragment>
      <p>
        {details.error ? (
          <Fragment>
            <b>Error:</b> {details.error}
          </Fragment>
        ) : (
          "Call succeeded"
        )}
      </p>
      <table className="enqueue-range-table">
        <thead>
          <tr className="enqueue-range-table__row enqueue-range-table__row--header">
            <th className="enqueue-range-table__cell enqueue-range-table__cell--header">
              Timestamp
            </th>
            <th className="enqueue-range-table__cell enqueue-range-table__cell--header">
              Message
            </th>
          </tr>
        </thead>
        <tbody>
          {details.events.map(event => (
            <tr className="enqueue-range-table__row--body">
              <td className="enqueue-range-table__cell enqueue-range-table__cell--date">
                {Print.Timestamp(event.time)}
              </td>
              <td className="enqueue-range-table__cell">
                <pre>{event.message}</pre>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Fragment>
  );
}

export function EnqueueRange({
  history,
}: EnqueueRangeAllProps): React.ReactElement {
  const searchParams = new URLSearchParams(history.location.search);
  const initialRangeID = searchParams.get("rangeID") || "";

  const [queue, setQueue] = useState(QUEUES[0]);
  const [rangeID, setRangeID] = useState(initialRangeID);
  const [nodeID, setNodeID] = useState("");
  const [skipShouldQueue, setSkipShouldQueue] = useState(false);
  const [response, setResponse] = useState<EnqueueRangeResponse>(null);
  const [error, setError] = useState<Error>(null);

  const handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    // Handle mixed-version clusters across the "gc" to "mvccGC" queue rename.
    // TODO(nvanbenschoten): remove this in v22.2. The server logic will continue
    // to map "gc" to "mvccGC" until v23.1.
    let q = queue;
    if (q === "mvccGC") {
      q = "gc";
    }
    const req = new EnqueueRangeRequest({
      queue: q,
      // These parseInts should succeed because <input type="number" />
      // enforces numeric input. Otherwise they're NaN.
      range_id: parseInt(rangeID, 10),
      node_id: parseInt(nodeID, 10),
      skip_should_queue: skipShouldQueue,
    });
    enqueueRange(req, moment.duration({ hours: 1 })).then(
      resp => {
        setResponse(resp);
        setError(null);
      },
      err => {
        setResponse(null);
        setError(err);
      },
    );
  };

  return (
    <Fragment>
      <Helmet title="Enqueue Range" />
      <BackToAdvanceDebug history={history} />
      <div className="content">
        <section className="section">
          <div className="form-container">
            <h1 className="base-heading heading">
              Manually enqueue range in a replica queue
            </h1>
            <br />
            <form
              onSubmit={handleSubmit}
              className="form-internal"
              method="post"
            >
              <label>
                <span className={"label-text"}>Queue:</span>
                <Dropdown
                  title=""
                  options={queueOptions}
                  selected={queue}
                  onChange={(selected: DropdownOption) =>
                    setQueue(selected.value)
                  }
                  className={"dropdown-area"}
                />
              </label>
              <br />
              <label>
                <span className={"label-text"}>RangeID:</span>
                <input
                  type="number"
                  name="rangeID"
                  className="input-text"
                  onChange={(evt: React.FormEvent<{ value: string }>) =>
                    setRangeID(evt.currentTarget.value)
                  }
                  value={rangeID}
                  placeholder="RangeID"
                />
              </label>
              <br />
              <label>
                <span className={"label-text"}>NodeID:</span>
                <input
                  type="number"
                  name="nodeID"
                  className="input-text"
                  onChange={(evt: React.FormEvent<{ value: string }>) =>
                    setNodeID(evt.currentTarget.value)
                  }
                  value={nodeID}
                  placeholder="NodeID (optional)"
                />
                <span className={"label-tooltip"}>
                  If not specified, we'll attempt to enqueue on all the nodes.
                </span>
              </label>
              <br />
              <label>
                <span className={"label-text"}>SkipShouldQueue:</span>
                <input
                  type="checkbox"
                  className="checkbox-area"
                  checked={skipShouldQueue}
                  name="skipShouldQueue"
                  onChange={() => setSkipShouldQueue(prev => !prev)}
                />
              </label>
              <br />
              <input type="submit" className="button-crl" value="Submit" />
            </form>
          </div>
        </section>
      </div>
      <section className="section">
        {response && (
          <Fragment>
            <h2 className="base-heading">Enqueue Range Output</h2>
            {response.details.map(details => (
              <div>
                <h3>Node n{details.node_id}</h3>
                {renderNodeResponse(details)}
              </div>
            ))}
          </Fragment>
        )}
        {error && (
          <Fragment>Error running EnqueueRange: {error.message}</Fragment>
        )}
      </section>
    </Fragment>
  );
}

export default withRouter(EnqueueRange);
