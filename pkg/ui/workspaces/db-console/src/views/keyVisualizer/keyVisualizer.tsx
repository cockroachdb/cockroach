// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import throttle from "lodash/throttle";
import React from "react";

import {
  CanvasHeight,
  CanvasWidth,
  MaxLabelsXAxis,
  MaxLabelsYAxis,
  MaxZoom,
  XAxisLabelPadding,
  YAxisLabelPadding,
} from "src/views/keyVisualizer/constants";
import {
  KeyVisualizerProps,
  SampleBucket,
} from "src/views/keyVisualizer/interfaces";

import { getRequestsAsNumber } from ".";

function drawBucket(
  pixels: Uint8ClampedArray,
  x: number,
  y: number,
  width: number,
  height: number,
  color: number[],
  showSpanBoundaries: boolean,
) {
  // clip if not on screen
  if (x > CanvasWidth || x + width < 0 || y > CanvasHeight || y + height < 0) {
    return;
  }

  for (let j = y; j < y + height; j++) {
    for (let i = x; i < x + width; i++) {
      // prevent wrap around indexing
      if (i < 0 || i >= CanvasWidth) {
        continue;
      }

      const index = i * 4 + j * 4 * CanvasWidth;

      // draw a gray border along the top and left of each cell
      if ((j === y + 1 || i === x) && showSpanBoundaries) {
        pixels[index] = 100; // red
        pixels[index + 1] = 100; // green
        pixels[index + 2] = 100; // blue
        pixels[index + 3] = 255; // alpha
      } else {
        // fill each cell normally
        pixels[index] = color[0] * 255; // red
        pixels[index + 1] = color[1] * 255; // green
        pixels[index + 2] = color[2] * 255; // blue
        pixels[index + 3] = 255; // alpha
      }
    }
  }
}

function filterAxisLabels(
  zoom: number,
  panOffset: number,
  offsets: Record<string, number>,
  maxLabels: number,
  canvasLength: number,
): Record<string, number> {
  // find y bounds of current view
  // find all labels that want to exist between these bounds
  // if that number <= max, do nothing
  // if > Max, reduce by factor of n / Max

  const zoomFactor = 1 / zoom; // percentage of the canvas you can see
  const windowSize = zoomFactor * canvasLength * MaxZoom;
  const min = zoomFactor * MaxZoom * -panOffset;
  const max = min + windowSize;

  const labelsInWindow = [] as string[];
  for (const [key, offset] of Object.entries(offsets)) {
    const offsetTransformed = offset * MaxZoom;
    if (offsetTransformed >= min && offsetTransformed <= max) {
      labelsInWindow.push(key);
    }
  }

  let labelsReduced = [] as string[];
  if (labelsInWindow.length > maxLabels) {
    // reduce by factor ceil(len / MaxLabels)
    const labelsToSkip = Math.ceil(labelsInWindow.length / maxLabels);
    for (let i = 0; i < labelsInWindow.length; i += labelsToSkip) {
      labelsReduced.push(labelsInWindow[i]);
    }
  } else {
    labelsReduced = labelsInWindow;
  }

  return labelsReduced.reduce(
    (acc, key) => {
      acc[key] = offsets[key];
      return acc;
    },
    {} as Record<string, number>,
  );
}

interface TooltipProps {
  x: number;
  y: number;
  startKey: string;
  endKey: string;
  requests: number;
  epochTime: number;
}

const BucketToolTip: React.FunctionComponent<TooltipProps> = props => {
  return (
    <div
      style={{
        pointerEvents: "none", // this prevents the canvas's onMouseOut from
        // firing if the tooltip itself is hovered by a fast-moving cursor.
        position: "absolute",
        left: `${props.x + 60}px`,
        top: `${props.y + 30}px`,
        background: "white",
        padding: "10px",
        borderRadius: "4px",
      }}
    >
      <p>start key: {props.startKey}</p>
      <p>end key: {props.endKey}</p>
      <p>requests: {props.requests}</p>
      <p>time: {new Date(props.epochTime * 1000).toUTCString()}</p>
    </div>
  );
};

type KeyVisualizerState = {
  tooltipState: TooltipProps;
  showTooltip: boolean;
  showSpanBoundaries: boolean;
};
export default class KeyVisualizer extends React.PureComponent<
  KeyVisualizerProps,
  KeyVisualizerState
> {
  canvasRef: React.RefObject<HTMLCanvasElement>;
  ctx: CanvasRenderingContext2D;
  xPanOffset = 0;
  yPanOffset = 0;
  xZoomFactor = 1;
  yZoomFactor = 1;
  throttledHandler: (e: React.MouseEvent) => void;

  state = {
    showTooltip: false,
    showSpanBoundaries: true,
    tooltipState: {
      x: 0,
      y: 0,
      startKey: "",
      endKey: "",
      requests: 0,
      epochTime: 0,
    },
  };
  private zoomHandlerThrottled: any;

  constructor(props: KeyVisualizerProps) {
    super(props);
    this.canvasRef = React.createRef();
  }

  componentDidMount() {
    this.ctx = this.canvasRef.current.getContext("2d");
  }

  computeBucketDimensions(sampleIndex: number, bucket: SampleBucket) {
    const startKey = this.props.keys[bucket.startKeyHex];
    const endKey = this.props.keys[bucket.endKeyHex];
    const nSamples = this.props.samples.length;

    const x =
      YAxisLabelPadding +
      this.xPanOffset +
      (sampleIndex * CanvasWidth * this.xZoomFactor) / nSamples;

    const y =
      this.props.yOffsetsForKey[startKey] * this.yZoomFactor + this.yPanOffset;

    const width =
      ((CanvasWidth - YAxisLabelPadding) * this.xZoomFactor) / nSamples;
    const height =
      this.props.yOffsetsForKey[endKey] * this.yZoomFactor -
      y +
      this.yPanOffset;

    return {
      x,
      y,
      width,
      height,
    };
  }

  renderYAxis() {
    // render y axis
    this.ctx.fillStyle = "white";
    this.ctx.font = "12px sans-serif";

    const yAxisLabels: Record<string, number> = filterAxisLabels(
      this.xZoomFactor,
      this.yPanOffset,
      this.props.yOffsetsForKey,
      MaxLabelsYAxis,
      CanvasHeight,
    );

    for (const [key, yOffset] of Object.entries(yAxisLabels)) {
      this.ctx.fillText(
        key,
        YAxisLabelPadding,
        yOffset * this.yZoomFactor + this.yPanOffset,
      );
    }
  }

  renderXAxis() {
    const xOffsetForSampleTime = this.props.samples.reduce(
      (acc, sample, index) => {
        const wallTimeMs = sample.timestamp.seconds.toNumber() * 1e3;
        const timeString = new Date(wallTimeMs).toISOString();
        const offset = (index * CanvasWidth) / this.props.samples.length;
        acc[timeString] = offset;
        return acc;
      },
      {} as Record<string, number>,
    );

    const xAxisLabels = filterAxisLabels(
      this.xZoomFactor,
      this.xPanOffset,
      xOffsetForSampleTime,
      MaxLabelsXAxis,
      CanvasWidth,
    );

    for (const [timestring, xOffset] of Object.entries(xAxisLabels)) {
      // split timestring and render each part
      const [s1, s2] = timestring.split("T");

      this.ctx.fillText(
        s1,
        YAxisLabelPadding + this.xPanOffset + xOffset * this.xZoomFactor,
        CanvasHeight - XAxisLabelPadding,
      );

      this.ctx.fillText(
        s2,
        YAxisLabelPadding + this.xPanOffset + xOffset * this.xZoomFactor,
        CanvasHeight - 2.5 * XAxisLabelPadding,
      );
    }
  }

  renderKeyVisualizer() {
    requestAnimationFrame(() => {
      // clear
      this.ctx.clearRect(0, 0, this.ctx.canvas.width, this.ctx.canvas.height);
      this.ctx.fillStyle = "black";
      this.ctx.fillRect(0, 0, this.ctx.canvas.width, this.ctx.canvas.height);

      const imageData = this.ctx.getImageData(
        0,
        0,
        this.ctx.canvas.width,
        this.ctx.canvas.height,
      );
      const pixels = imageData.data;

      // render samples
      // render samples
      const nSamples = this.props.samples.length;
      for (let i = 0; i < nSamples; i++) {
        const sample = this.props.samples[i];

        for (let j = 0; j < sample.buckets.length; j++) {
          const bucket = sample.buckets[j];

          // compute x, y, width, and height of rendered span.
          const dim = this.computeBucketDimensions(i, bucket);

          const { x, y, width, height } = dim;

          // compute color
          const color = [
            Math.log(Math.max(getRequestsAsNumber(bucket.requests), 1)) /
              Math.log(this.props.hottestBucket),
            0,
            0,
          ];

          drawBucket(
            pixels,
            Math.ceil(x),
            Math.ceil(y),
            Math.ceil(width),
            Math.ceil(height),
            color,
            this.state.showSpanBoundaries,
          );
        }
      }

      // blit
      this.ctx.putImageData(imageData, 0, 0);

      // render x and y axis
      this.renderYAxis();
      this.renderXAxis();
    });
  }

  componentDidUpdate(prevProps: KeyVisualizerProps) {
    // only render if the sample window changes.
    // prevent render if there's a tooltip state change.
    if (prevProps !== this.props) {
      this.renderKeyVisualizer();
    }
  }

  handleCanvasScroll = (e: any) => {
    if (!this.zoomHandlerThrottled) {
      this.zoomHandlerThrottled = throttle((e: any) => {
        // normalize value and negate so that "scrolling up" zooms in
        const deltaY = -e.deltaY / 100;

        this.yZoomFactor += deltaY;
        this.xZoomFactor += deltaY;

        // clamp zoom factor between 1 and MaxZoom
        this.yZoomFactor = Math.max(1, Math.min(MaxZoom, this.yZoomFactor));
        this.xZoomFactor = Math.max(1, Math.min(MaxZoom, this.xZoomFactor));

        // find mouse coordinates in terms of current window
        const windowPercentageX = e.nativeEvent.offsetX / CanvasWidth;
        const windowPercentageY = e.nativeEvent.offsetY / CanvasHeight;

        const z =
          this.xZoomFactor === 1 ? 0 : (this.xZoomFactor - 1) / (MaxZoom - 1);
        this.xPanOffset = windowPercentageX * CanvasWidth * MaxZoom * z * -1;
        this.yPanOffset = windowPercentageY * CanvasHeight * MaxZoom * z * -1;

        // if zoomed out, reset pan
        if (this.yZoomFactor === 1 && this.xZoomFactor === 1) {
          this.xPanOffset = 0;
          this.yPanOffset = 0;
        }

        this.renderKeyVisualizer();
      }, 1000 / 60);
    }

    this.zoomHandlerThrottled(e);
  };

  handleCanvasHover = (e: React.MouseEvent) => {
    if (!this.throttledHandler) {
      this.throttledHandler = throttle(e => {
        const rect = this.canvasRef.current.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;
        const nSamples = this.props.samples.length;

        // label this for loop so we can break from it.
        // I thought this would need to be implemented with some sort of O(1) lookup
        // or a binary partitioning scheme, but a naive `for` loop seems to be fast enough...
        iterate_samples: for (let i = 0; i < nSamples; i++) {
          const sample = this.props.samples[i];

          for (let j = 0; j < sample.buckets.length; j++) {
            const bucket = sample.buckets[j];

            const { x, y, width, height } = this.computeBucketDimensions(
              i,
              bucket,
            );

            if (
              mouseX >= x &&
              mouseX <= x + width &&
              mouseY >= y &&
              mouseY <= y + height
            ) {
              this.setState({
                tooltipState: {
                  x: x + width + 20,
                  y: y,
                  startKey: this.props.keys[bucket.startKeyHex],
                  endKey: this.props.keys[bucket.endKeyHex],
                  requests: getRequestsAsNumber(bucket.requests),
                  epochTime: sample.timestamp.seconds.toNumber(),
                },
              });
              break iterate_samples;
            }
          } // end iterate buckets
        } // end iterate_samples
      }, 50);
    }

    this.throttledHandler(e);
  };

  toggleShowSpanBoundaries = () => {
    this.setState(
      { showSpanBoundaries: !this.state.showSpanBoundaries },
      () => {
        this.renderKeyVisualizer();
      },
    );
  };

  render() {
    return (
      <>
        <canvas
          onWheel={e => {
            e.persist();
            this.handleCanvasScroll(e);
          }}
          onMouseEnter={() => this.setState({ showTooltip: true })}
          onMouseOut={() => this.setState({ showTooltip: false })}
          onMouseMove={e => {
            e.persist();
            this.handleCanvasHover(e);
          }}
          width={CanvasWidth}
          height={CanvasHeight}
          ref={this.canvasRef}
        />
        {this.state.showTooltip && (
          <BucketToolTip {...this.state.tooltipState} />
        )}
        <div>
          <label>
            <input
              type="checkbox"
              checked={this.state.showSpanBoundaries}
              onChange={() => this.toggleShowSpanBoundaries()}
            />{" "}
            Show span boundaries
          </label>
        </div>
      </>
    );
  }
}
