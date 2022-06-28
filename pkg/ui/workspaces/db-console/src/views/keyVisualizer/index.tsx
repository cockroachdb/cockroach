import React from "react";
import { CanvasHeight, XAxisLabelPadding } from "./constants";
import { GetSamplesResponse, SpanStatistics } from "./interfaces";
import { KeyVisualizer } from "./keyVisualizer";
import {
  SpanDetailTooltip,
  SpanDetailTooltipProps,
} from "oss/src/views/keyVisualizer/spanDetailTooltip";
import { RESET_SQL_STATS_COMPLETE } from "oss/src/redux/sqlStats";

interface KeyVisualizerPageState {
  response: GetSamplesResponse;
  yOffsetForKey: Record<string, number>;
  highestBatchRequests: number;
  showTooltip: boolean;
  tooltip: SpanDetailTooltipProps;
}

export default class KeyVisualizerPage extends React.Component<
  {},
  KeyVisualizerPageState
> {
  state = {
    response: { samples: [] as any, keys: [] as any },
    yOffsetForKey: {},
    highestBatchRequests: 1,
    showTooltip: true,
    tooltip: { x: 0, y: 0, time: "", spanStats: undefined as any },
  };

  // processResponse does 3 things:
  // 1) finds the highest `batchRequests` value contained within all samples
  // 2) computes the y-offset for each key in the keyspace
  // 3) writes these values and the response to state, for consumption by the visualizer.
  processResponse(response: GetSamplesResponse) {
    let highestBatchRequests = 0;
    let highestBytes = 0;

    for (const sample of response.samples) {
      for (const stat of sample.spanStats) {
        // hack to deal with qps -> batchRequests transition
        // hack to deal with `0` values being left out by json marshaller.
        if (stat.batchRequests === undefined) {
          stat.batchRequests = 0;
        } else {
          // hack to deal with json marshaller encoding integers as strings.
          stat.batchRequests = parseInt(String(stat.batchRequests));
        }

        if (stat.nBytes === undefined) {
          stat.nBytes = 0
        } else {
          stat.nBytes = parseInt(String(stat.nBytes));
        }


        if (stat.nBytes > highestBytes) {
          highestBytes = stat.nBytes
        }
      }
    }


    for (let sample of response.samples) {
      for (const stat of sample.spanStats) {
        const normalizedBytes = stat.nBytes / highestBytes;
        if (normalizedBytes !== 0) {
          stat.batchRequestsNormalized = stat.batchRequests * normalizedBytes;
        } else {
          stat.batchRequestsNormalized = 0; // TODO: revisit this.
        }

        // maintain highest batch request value
        if (stat.batchRequestsNormalized > highestBatchRequests) {
          highestBatchRequests = stat.batchRequestsNormalized;
        }
      }
    }


    // compute height of each key
    const yOffsetForKey = response.keys.reduce((acc, curr, index) => {
      acc[curr] =
        (index * (CanvasHeight - XAxisLabelPadding)) /
        (response.keys.length - 1);
      return acc;
    }, {} as any);

    console.log("response: ", response);
    console.log("key offsets: ", yOffsetForKey);
    console.log("highest bytes: ", highestBytes);
    console.log("highest batch requests: ", highestBatchRequests);

    this.setState({
      response,
      yOffsetForKey,
      highestBatchRequests,
    });
  }

  componentDidMount() {
    fetch("http://localhost:8080/_status/v1/key-visualizer")
      .then(res => res.json())
      .then(response => this.processResponse(response));
  }

  updateSpanDetailTooltip = (
    x: number,
    y: number,
    time: string,
    spanStats: SpanStatistics,
  ) => {
    this.setState({ tooltip: { x, y, time, spanStats } });
  };

  render() {
    return (
      <div style={{ position: "relative" }}>
        <KeyVisualizer
          response={this.state.response}
          yOffsetForKey={this.state.yOffsetForKey}
          highestTemp={this.state.highestBatchRequests}
          setShowTooltip={show => this.setState({ showTooltip: show })}
          setTooltipDetails={this.updateSpanDetailTooltip}
        />
        {this.state.showTooltip && (
          <SpanDetailTooltip {...this.state.tooltip} />
        )}
      </div>
    );
  }
}
