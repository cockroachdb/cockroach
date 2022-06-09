import React from "react";
import { CanvasHeight, XAxisLabelPadding } from "./constants";
import { GetSamplesResponse, SpanStatistics } from "./interfaces";
import { KeyVisualizer } from "./keyVisualizer";
import { SpanDetailTooltip, SpanDetailTooltipProps } from "oss/src/views/keyVisualizer/spanDetailTooltip";

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
    for (const sample of response.samples) {
      for (const stat of sample.spanStats) {
        // hack to deal with qps -> batchRequests transition
        if ((stat as any).qps === undefined) {
          stat.batchRequests = 0;
        } else {
          // hack to deal with `0` values being left out by json marshaller.
          stat.batchRequests = parseInt((stat as any).qps);
        }

        // maintain highest batch request value
        if (stat.batchRequests > highestBatchRequests) {
          highestBatchRequests = stat.batchRequests;
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

    console.log(response);
    console.log(yOffsetForKey);
    console.log(highestBatchRequests);

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
