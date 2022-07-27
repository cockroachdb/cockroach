// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import {cockroach} from "src/js";
import {getKeyVisualizerSamples} from "src/util/api";
import KeyVisualizer from "src/views/keyVisualizer/keyVisualizer";
import {KeyVisualizerProps} from "src/views/keyVisualizer/interfaces";
import {CanvasHeight, XAxisLabelPadding} from "./constants";
import {KeyVisualizerTimeWindow} from "src/views/keyVisualizer/timeWindow";
import {AdminUIState} from "src/redux/state";
import {connect} from "react-redux";
import {TimeScale, util} from "@cockroachlabs/cluster-ui";
import {selectClusterSettings} from "src/redux/clusterSettings";
import {selectTimeScale} from "src/redux/timeScale";
import {refreshSettings} from "src/redux/apiReducers";
import KeyVisSamplesRequest = cockroach.server.serverpb.KeyVisSamplesRequest;
import KeyVisSamplesResponse = cockroach.server.serverpb.KeyVisSamplesResponse;

const EnabledSetting = "keyvisualizer.job.enabled"
const IntervalSetting = "keyvisualizer.job.sample_interval"


function highestBucket(samples: KeyVisSamplesResponse["samples"]) {
  let highest = 0;
  for (let sample of samples) {
    for (let stat of sample.buckets) {
      const numRequests = stat.requests?.toInt() || 0
      if (numRequests > highest) {
        highest = numRequests;
      }
    }
  }
  return highest
}


interface KeyVisualizerContainerState {
  response: KeyVisSamplesResponse;
}

function buildKeyVisualizerProps(state: KeyVisualizerContainerState, timeScale: TimeScale): KeyVisualizerProps {

  // A note about time and units:
  // The time window is in units of seconds.
  // hlc.Timestamp.wall_time is in units of nanoseconds.

  const windowEndSeconds = timeScale.fixedWindowEnd === false
    ? Date.now() / 1000
    : timeScale.fixedWindowEnd.unix()

  const windowStartSeconds = windowEndSeconds - timeScale.windowSize.asSeconds()


  // filter out samples that fall outside of the window
  // compute highestBucket
  const samples = state.response.samples
    .filter((sample) => {
      const sampleTime = sample.timestamp.wall_time.toNumber() / 1e9;
      return sampleTime >= windowStartSeconds && sampleTime <= windowEndSeconds;
    })
    .sort(
      (a, b) =>
        a.timestamp.wall_time.toNumber() - b.timestamp.wall_time.toNumber()
    );


  let highestBucketInCurrentWindow = highestBucket(samples)

  // TODO(zachlite): make this faster
  // const sortedPrettyKeysInWindow = state.response.sorted_pretty_keys.filter((prettyKey) => {
  //   // is this key in the time window?
  //   for (const sample of samples) {
  //     for (const bucket of sample.buckets) {
  //
  //       const startPretty = state.response.pretty_key_for_uuid[bucket.start_key_id]
  //       const endPretty = state.response.pretty_key_for_uuid[bucket.end_key_id]
  //
  //       if (startPretty === prettyKey || endPretty === prettyKey) {
  //         return true
  //       }
  //     }
  //   }
  //   return false;
  // })

  // compute y offset for each key
  const yOffsetsForKey = {} as Record<string, number>;
  state.response.sorted_pretty_keys.forEach((key, i) => {
    yOffsetsForKey[key] =
      (i * (CanvasHeight - XAxisLabelPadding)) /
      (state.response.sorted_pretty_keys.length - 1);
  });

  console.log(samples)

  return {
    keys: state.response.pretty_key_for_uuid,
    samples,
    yOffsetsForKey,
    highestBucketInCurrentWindow
  }
}


interface KeyVisualizerContainerProps {
  refreshInterval: number;
  timeScale: TimeScale
}

class KeyVisualizerContainer extends React.Component<KeyVisualizerContainerProps, KeyVisualizerContainerState> {
  interval: any

  state = {response: KeyVisSamplesResponse.create()}

  fetchSamples() {
    const req = KeyVisSamplesRequest.create({})
    getKeyVisualizerSamples(req).then(res => this.setState({response: res}));
  }

  componentDidMount() {
    // set up a recurring sample refresh
    this.interval = setInterval(() => {
      this.fetchSamples()
    }, this.props.refreshInterval);

    // do an initial fetch
    this.fetchSamples()
  }

  componentWillUnmount() {
    clearInterval(this.interval)
  }

  render() {
    const {
      samples,
      yOffsetsForKey,
      highestBucketInCurrentWindow,
      keys
    } = buildKeyVisualizerProps(this.state, this.props.timeScale);

    return <div style={{position: "relative"}}>
      <KeyVisualizerTimeWindow/>
      <KeyVisualizer samples={samples}
                     yOffsetsForKey={yOffsetsForKey}
                     highestBucketInCurrentWindow={highestBucketInCurrentWindow}
                     keys={keys}/>
    </div>
  }
}


// goals:
// 1) if settings are undefined, refreshSettings
// 2) if key visualizer job is not enabled, do not render the key visualizer
// 3) if key visualizer job is enabled, use sample period setting to poll
// for new data

interface KeyVisualizerPageProps {
  clusterSettings?: { [key: string]: cockroach.server.serverpb.SettingsResponse.IValue }
  refreshSettings: typeof refreshSettings;
  timeScale: TimeScale
}

const KeyVisualizerPage: React.FunctionComponent<KeyVisualizerPageProps> = (props) => {
  if (props.clusterSettings === undefined) {
    props.refreshSettings()
    return null;
  }

  const enabled = props.clusterSettings[EnabledSetting].value === "true"

  if (!enabled) {
    return <div>
      <p>To enable the key visualizer, run the following SQL statement:</p>
      <pre>SET CLUSTER SETTING keyvisualizer.job.enabled = true;</pre>
    </div>
  }

  const refreshInterval = util.durationFromISO8601String(
    props.clusterSettings[IntervalSetting].value
  ).asMilliseconds()

  return <KeyVisualizerContainer timeScale={props.timeScale}
                                 refreshInterval={refreshInterval}/>
}


export default connect(
  (state: AdminUIState) => ({
    clusterSettings: selectClusterSettings(state),
    timeScale: selectTimeScale(state),
  }),
  {
    refreshSettings
  },
)(KeyVisualizerPage);
