// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React, { Fragment } from "react";
import Helmet from "react-helmet";
import { RouteComponentProps, withRouter } from "react-router-dom";

import { cockroach } from "src/js/protos";
import { enqueueRange } from "src/util/api";
import Print from "src/views/reports/containers/range/print";
import { BackToAdvanceDebug } from "src/views/reports/containers/util";
import Dropdown, { DropdownOption } from "src/views/shared/components/dropdown";

import "./index.styl";

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

interface EnqueueRangeProps {
  handleEnqueueRange: (
    queue: string,
    rangeID: number,
    nodeID: number,
    skipShouldQueue: boolean,
  ) => Promise<EnqueueRangeResponse>;
}

interface EnqueueRangeState {
  queue: string;
  rangeID: string;
  nodeID: string;
  skipShouldQueue: boolean;
  response: EnqueueRangeResponse;
  error: Error;
}

export type EnqueueRangeAllProps = EnqueueRangeProps & RouteComponentProps;

export class EnqueueRange extends React.Component<
  EnqueueRangeAllProps,
  EnqueueRangeState
> {
  constructor(props: EnqueueRangeAllProps) {
    super(props);
    const { history } = this.props;
    const searchParams = new URLSearchParams(history.location.search);
    const rangeID = searchParams.get("rangeID") || "";

    this.state = {
      queue: QUEUES[0],
      rangeID: rangeID,
      nodeID: "",
      skipShouldQueue: false,
      response: null,
      error: null,
    };
  }

  handleUpdateQueue = (selectedOption: DropdownOption) => {
    this.setState({
      queue: selectedOption.value,
    });
  };

  handleUpdateRangeID = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      rangeID: evt.currentTarget.value,
    });
  };

  handleUpdateNodeID = (evt: React.FormEvent<{ value: string }>) => {
    this.setState({
      nodeID: evt.currentTarget.value,
    });
  };

  handleEnqueueRange = (
    queue: string,
    rangeID: number,
    nodeID: number,
    skipShouldQueue: boolean,
  ) => {
    // Handle mixed-version clusters across the "gc" to "mvccGC" queue rename.
    // TODO(nvanbenschoten): remove this in v22.2. The server logic will continue
    // to map "gc" to "mvccGC" until v23.1.
    if (queue === "mvccGC") {
      queue = "gc";
    }
    const req = new EnqueueRangeRequest({
      queue: queue,
      range_id: rangeID,
      node_id: nodeID,
      skip_should_queue: skipShouldQueue,
    });
    return enqueueRange(req, moment.duration({ hours: 1 }));
  };

  handleSubmit = (evt: React.FormEvent<any>) => {
    evt.preventDefault();

    this.handleEnqueueRange(
      this.state.queue,
      // These parseInts should succeed because <input type="number" />
      // enforces numeric input. Otherwise they're NaN.
      parseInt(this.state.rangeID, 10),
      parseInt(this.state.nodeID, 10),
      this.state.skipShouldQueue,
    ).then(
      response => {
        this.setState({ response, error: null });
      },
      error => {
        this.setState({ response: null, error });
      },
    );
  };

  renderNodeResponse(details: EnqueueRangeResponse.IDetails) {
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

  renderResponse() {
    const { response } = this.state;

    if (!response) {
      return null;
    }

    return (
      <Fragment>
        <h2 className="base-heading">Enqueue Range Output</h2>
        {response.details.map(details => (
          <div>
            <h3>Node n{details.node_id}</h3>

            {this.renderNodeResponse(details)}
          </div>
        ))}
      </Fragment>
    );
  }

  renderError() {
    const { error } = this.state;

    if (!error) {
      return null;
    }

    return <Fragment>Error running EnqueueRange: {error.message}</Fragment>;
  }

  render() {
    return (
      <Fragment>
        <Helmet title="Enqueue Range" />
        <BackToAdvanceDebug history={this.props.history} />
        <div className="content">
          <section className="section">
            <div className="form-container">
              <h1 className="base-heading heading">
                Manually enqueue range in a replica queue
              </h1>
              <br />
              <form
                onSubmit={this.handleSubmit}
                className="form-internal"
                method="post"
              >
                <label>
                  <span className={"label-text"}>Queue:</span>
                  <Dropdown
                    title=""
                    options={queueOptions}
                    selected={this.state.queue}
                    onChange={this.handleUpdateQueue}
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
                    onChange={this.handleUpdateRangeID}
                    value={this.state.rangeID}
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
                    onChange={this.handleUpdateNodeID}
                    value={this.state.nodeID}
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
                    checked={this.state.skipShouldQueue}
                    name="skipShouldQueue"
                    onChange={() =>
                      this.setState({
                        skipShouldQueue: !this.state.skipShouldQueue,
                      })
                    }
                  />
                </label>
                <br />
                <input type="submit" className="button-crl" value="Submit" />
              </form>
            </div>
          </section>
        </div>
        <section className="section">
          {this.renderResponse()}
          {this.renderError()}
        </section>
      </Fragment>
    );
  }
}

const EnqueueRangeConnected = withRouter(EnqueueRange);

export default EnqueueRangeConnected;
