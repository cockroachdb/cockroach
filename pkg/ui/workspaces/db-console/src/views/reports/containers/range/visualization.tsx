// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, {useEffect} from "react";
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

class RangeVizCanvas extends React.Component<RangeVizCanvasProps> {
  render() {
    return <div>
      <div>{this.props.raftData ? Object.keys(this.props.raftData?.ranges) : "nothing here yet..."}</div>
      <canvas></canvas>
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
