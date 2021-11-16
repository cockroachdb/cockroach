// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, {createRef, useEffect} from "react";
import { RaftDebugResponseMessage } from "src/util/api";
import { refreshRaft } from "oss/src/redux/apiReducers";
import { AdminUIState } from "oss/src/redux/state";
import { connect } from "react-redux";

interface RangeVizProps {
  refreshRaft: typeof refreshRaft;
  raftData: RaftDebugResponseMessage;
}

interface RangeVizCanvasProps {
  raftData: RaftDebugResponseMessage;
}


const CanvasWidth = 800;
const CanvasHeight = 300;
const HotColor = [4, 242, 235];
const ColdColor = [13, 6, 34];
const MaxTimestepsShown = 10;
class Canvas extends React.Component<{
  canvasRef: React.RefObject<HTMLCanvasElement>;
}> {
  shouldComponentUpdate() {
    return false;
  }

  render() {
    return <canvas ref={this.props.canvasRef} width={CanvasWidth} height={CanvasHeight}></canvas>;
  }
}

function lerp(a: number, b: number, t: number) {
  return (1 - t) * a + t * b;
}

function lerpColor(c1: number[], c2: number[], t: number) {
  return [lerp(c1[0], c2[0], t), lerp(c1[1], c2[1], t), lerp(c1[2], c2[2], t)];
}

class RangeVizCanvas extends React.Component<RangeVizCanvasProps> {
  canvasRef: React.RefObject<HTMLCanvasElement>;
  drawContext: CanvasRenderingContext2D;
  
  constructor(props: RangeVizCanvasProps) {
    super(props);
    this.canvasRef = createRef();
  }

  fakeRangesOverTime() {
    const rangesOverTime = [];
    const nTimeRanges = 4;
    const nRanges = 12;

    for (let timeIdx = 0; timeIdx< nTimeRanges; timeIdx++) {
      const ranges = [];
      for (let rangeIdx = 0; rangeIdx < nRanges; rangeIdx++) {
        ranges.push({rangeId: 0, qps: Math.random()})
      }
      rangesOverTime.push(ranges)
    }
    return rangesOverTime;
  }

  drawHeatMap() {
    // clear canvas
    this.drawContext.clearRect(0,0, CanvasWidth, CanvasHeight);
    
    // draw background
    this.drawContext.fillStyle = `rgb(${ColdColor[0]}, ${ColdColor[1]}, ${ColdColor[2]})`;
    this.drawContext.fillRect(0, 0, CanvasWidth, CanvasHeight);

    // fake data for now
    const rangesOverTime = this.fakeRangesOverTime();
    const cellHeight = CanvasHeight / rangesOverTime[0].length;
    const cellWidth = CanvasWidth / MaxTimestepsShown;
    
    const highestQPS = Math.max(...rangesOverTime.flatMap(ranges => ranges.map(r => r.qps)));

    for (let timeIdx = 0; timeIdx < rangesOverTime.length; timeIdx++) {
      for (let rangeIdx = 0; rangeIdx < rangesOverTime[timeIdx].length; rangeIdx++) {
        
        // compute cell color by considering this range's QPS relative 
        // to the max QPS found across all ranges.
        const t = rangesOverTime[timeIdx][rangeIdx].qps / highestQPS;
        const [r, g, b] = lerpColor(ColdColor, HotColor, t);

        // draw cell
        this.drawContext.fillStyle = `rgb(${r}, ${g}, ${b})`;
        this.drawContext.fillRect(
          cellWidth * timeIdx,
          rangeIdx * cellHeight,
          cellWidth,
          cellHeight
        );
      }
    }
  }

  componentDidMount() {
    this.drawContext = this.canvasRef.current.getContext("2d");
    this.drawHeatMap();
    // TODO(zachlite):
    // 1) convert real data from props into {rangeId, qps}[][], as mocked by `fakeRangesOverTime`
    // 2) in componentDidUpdate, save (append) latest range data
    // 3) after receipt of > 10th range, throw away n - 10th range.
    // 4) axis labels for timestamp and range id
    // 5) show range statistics on cell mouseover
  }

  componentDidUpdate() {}

  render() {
    console.log(this.props.raftData)
    return <div>
      <div>{this.props.raftData ? Object.keys(this.props.raftData?.ranges) : "nothing here yet..."}</div>
      <Canvas canvasRef={this.canvasRef}></Canvas>
    </div>;
  }
}

const RangeViz: React.FC<RangeVizProps> = props => {
  useEffect(() => {
    props.refreshRaft();
  }, []);

  return (
    <div>
      <RangeVizCanvas raftData={props.raftData} />
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
