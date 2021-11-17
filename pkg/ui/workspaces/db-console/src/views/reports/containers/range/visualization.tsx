// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { createRef, useEffect } from "react";
import { RaftDebugResponseMessage } from "src/util/api";
import { refreshRaft } from "oss/src/redux/apiReducers";
import { AdminUIState } from "oss/src/redux/state";
import { connect } from "react-redux";
import _ from "lodash";
import moment from "moment";
import * as d3Chromatic from "d3-scale-chromatic"

interface RangeVizProps {
  refreshRaft: typeof refreshRaft;
  raftData: RaftDebugResponseMessage;
}

type RaftData = {
  rangeId: number;
  qps: number;
  startKey: string;
  endKey: string;
  liveBytes: number;
};
type RaftUpdates = {raftData: RaftData[], timestamp: string};
interface RangeVizCanvasProps {
  raftData: RaftUpdates;
}

interface RangeVizCanvasState {
  hoverData: RaftData;
  timestamps: string[]
  ranges: {startKey: string, rangeHeightPx: number}[]
}

const CanvasWidth = 1200;
const CanvasHeight = 800;
const MaxTimestepsShown = 20;
class Canvas extends React.Component<{
  canvasRef: React.RefObject<HTMLCanvasElement>;
}> {
  shouldComponentUpdate() {
    return false;
  }

  render() {
    return (
      <canvas
        ref={this.props.canvasRef}
        width={CanvasWidth}
        height={CanvasHeight}
      ></canvas>
    );
  }
}

// function newFakeRanges() {
//   const nRanges = 100;
//   const ranges = [];
//   for (let rangeIdx = 0; rangeIdx < nRanges; rangeIdx++) {
//     ranges.push({rangeId: 0, qps: Math.random()})
//   }
//   return ranges;
// }


interface TimeAxisProps {
  timestamps: string[];
};

interface RangeAxisProps {
  ranges: {startKey: string, rangeHeightPx: number}[];
}

class RangeAxis extends React.PureComponent<RangeAxisProps> {
  render() {
      return <div>
        {this.props.ranges.map((range, i) => {
          return <div style={{marginTop: range.rangeHeightPx}} key={i}>{range.startKey}</div>
        })}
      </div>
  }
}

class TimeAxis extends React.PureComponent<TimeAxisProps> {
  render () {
    return <div style={{display: "flex"}}>
      {this.props.timestamps.map((timestamp, i) => {
        return (
          <div
            style={{marginLeft: (CanvasWidth / MaxTimestepsShown) - 20, writingMode: "vertical-lr", textOrientation: "sideways" }}
            key={i}
          >
            {timestamp}
          </div>
        );
      })}
    </div>
  }
}

class RangeVizCanvas extends React.Component<
  RangeVizCanvasProps,
  RangeVizCanvasState
> {
  canvasRef: React.RefObject<HTMLCanvasElement>;
  drawContext: CanvasRenderingContext2D;
  rangeUpdates: RaftUpdates[];
  highestQPS: number;

  constructor(props: RangeVizCanvasProps) {
    super(props);
    this.canvasRef = createRef();
    this.rangeUpdates = [];
    this.highestQPS = 0;
    this.state = {
      hoverData: {
        qps: 0,
        startKey: "",
        endKey: "",
        rangeId: 0,
        liveBytes: 0
      },
      timestamps: [],
      ranges: []
    };
  }

  drawHeatMap() {
    // clear canvas
    this.drawContext.clearRect(0, 0, CanvasWidth, CanvasHeight);

    // // draw background
    this.drawContext.fillStyle = '#000';
    this.drawContext.fillRect(0, 0, CanvasWidth, CanvasHeight);

    if (this.rangeUpdates.length === 0) {
      return;
    }

    // fake data for now
    const rangesOverTime = this.rangeUpdates;
    const cellWidth = CanvasWidth / MaxTimestepsShown;
    const cellHeight = CanvasHeight / this.rangeUpdates[0].raftData.length;

    this.highestQPS = Math.max(
      ...rangesOverTime.flatMap(ranges => ranges.raftData.map(r => r.qps)),
      // this.highestQPS
    );

    for (let timeIdx = 0; timeIdx < rangesOverTime.length; timeIdx++) {
      for (
        let rangeIdx = 0;
        rangeIdx < rangesOverTime[timeIdx].raftData.length;
        rangeIdx++
      ) {
        // compute cell color by considering this range's QPS relative
        // to the max QPS found across all ranges.
        const t = rangesOverTime[timeIdx].raftData[rangeIdx].qps / this.highestQPS;
        const colorString = d3Chromatic.interpolateRdYlBu(1-t); 

        // draw cell
        this.drawContext.fillStyle = colorString;
        this.drawContext.fillRect(
          cellWidth * timeIdx,
          rangeIdx * cellHeight,
          cellWidth,
          cellHeight,
        );
      }
    }
  }

  componentDidMount() {
    this.drawContext = this.canvasRef.current.getContext("2d");
    this.installMouseHandler();
    this.drawHeatMap();
    // TODO(zachlite):
    // 1) [DONE] convert real data from props into {rangeId, qps}[][], as mocked by `fakeRangesOverTime`
    // 2) [DONE] in componentDidUpdate, save (append) latest range data
    // 3) [DONE] after receipt of > 10th range, throw away n - 10th range.
    // 4) axis labels for timestamp [DONE] and range start key
    // 5) [DONE] show range statistics on cell mouseover
    // 6) make cell height proportional to total bytes
  }

  installMouseHandler() {
    // TODO: clean this listener up.
    this.canvasRef.current.addEventListener("mousemove", e => {
      if (this.rangeUpdates.length === 0) {
        return;
      }

      const updateIdx = Math.floor(
        Math.max(0, Math.min(CanvasWidth, e.offsetX)) /
          (CanvasWidth / MaxTimestepsShown),
      );

      if (updateIdx > this.rangeUpdates.length - 1) {
        return;
      }

      const rangeIdx = Math.floor(
        Math.max(0, Math.min(CanvasHeight, e.offsetY)) /
          (CanvasHeight / this.rangeUpdates[0].raftData.length),
      );

      this.setState({
        hoverData: this.rangeUpdates[updateIdx].raftData[rangeIdx],
      });
    });
  }

  updateHeatMap() {
    // get rid of oldest update so heatmap appears to advance right over time.
    if (this.rangeUpdates.length >= MaxTimestepsShown) {
      this.rangeUpdates.shift();
    }

    // save this new time range.
    this.rangeUpdates.push(this.props.raftData);

    this.setState({
      timestamps: this.rangeUpdates.map((update) => update.timestamp),
      ranges: this.rangeUpdates[0].raftData.map((range, i) => ({
        startKey: range.startKey,
        rangeHeightPx: 20,
      })),
    });

    // re-draw heatmap
    this.drawHeatMap();
  }

  componentDidUpdate(prevProps: RangeVizCanvasProps) {
    // Only update the canvas when props change.
    // Internal state changes should cause a re-render,
    // but should not update the canvas.
    // TODO(zachlite): re-evaluate this component for proper separation of concerns.
    if (!_.isEqual(prevProps.raftData, this.props.raftData)) {
      this.updateHeatMap();
    }
  }

  render() {
    return (
      <>
        <RangeAxis ranges={this.state.ranges}/>
        <Canvas canvasRef={this.canvasRef}></Canvas>
        <TimeAxis timestamps={this.state.timestamps}/>
        <div>
          <h3>Range Info:</h3>
          <ul>
            <li>QPS: {this.state.hoverData?.qps}</li>
            <li>Start Key: {this.state.hoverData?.startKey}</li>
            <li>End Key: {this.state.hoverData?.endKey}</li>
            <li>Live Bytes: {this.state.hoverData?.liveBytes}</li>
          </ul>
        </div>
      </>
    );
  }
}

const RangeViz: React.FC<RangeVizProps> = props => {
  useEffect(() => {
    const refreshInterval = setInterval(() => props.refreshRaft(), 3000);
    return () => {
      clearInterval(refreshInterval);
    };
  }, []);

  const raftUpdates: RaftUpdates = props.raftData
    ? {raftData: Object.values(props.raftData.ranges).map(d => {
        return {
            rangeId: d.range_id.toInt(),
            qps: d.nodes[0].range.stats.queries_per_second,
            startKey: d.nodes[0].range.span.start_key,
            endKey: d.nodes[0].range.span.end_key,
            liveBytes: d.nodes[0].range.state.state.stats.live_bytes.toInt(),
        };
      }), timestamp: moment.utc().format("HH:mm:ss UTC")}
    : {raftData: [], timestamp: undefined};

  return (
    <div>
      <RangeVizCanvas raftData={raftUpdates} />
    </div>
  );
};

export const RangeVizConnected = connect(
  (state: AdminUIState) => {
    return {
      raftData: (state => {
        return state.cachedData.raft.data;
      })(state),
    };
  },
  {
    refreshRaft,
  },
)(RangeViz);
