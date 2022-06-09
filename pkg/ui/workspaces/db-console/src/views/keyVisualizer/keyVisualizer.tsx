import { throttle } from "lodash";
import React from "react";
import {
  CanvasWidth,
  CanvasHeight,
  YAxisLabelPadding,
  XAxisLabelPadding,
} from "./constants";
import { KeyVisualizerProps, SpanStatistics } from "./interfaces";

function drawBucket(
  pixels: any,
  x: number,
  y: number,
  width: number,
  height: number,
  color: number[],
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

      if (j === y || i === x) {
        pixels[index] = 100; // red
        pixels[index + 1] = 100; // green
        pixels[index + 2] = 100; // blue
        pixels[index + 3] = 255; // alpha
      } else {
        pixels[index] = color[0] * 255; // red
        pixels[index + 1] = color[1] * 255; // green
        pixels[index + 2] = color[2] * 255; // blue
        pixels[index + 3] = 255; // alpha
      }
    }
  }
}

export class KeyVisualizer extends React.PureComponent<KeyVisualizerProps> {
  xPanOffset = 0;
  yPanOffset = 0;
  isPanning = false;
  yZoomFactor = 1;
  xZoomFactor = 1;

  canvasRef: React.RefObject<HTMLCanvasElement>;
  ctx: CanvasRenderingContext2D;
  panHandlerThrottled: (
    e: React.MouseEvent<HTMLCanvasElement, MouseEvent>,
  ) => void;
  zoomHandlerThrottled: (e: React.WheelEvent<HTMLCanvasElement>) => void;
  hoverHandlerThrottled: any;

  constructor(props: KeyVisualizerProps) {
    super(props);
    this.canvasRef = React.createRef();
  }

  renderKeyVisualizer = () => {
    requestAnimationFrame(() => {
      const startTime = window.performance.now();
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
      const nSamples = this.props.response.samples.length || 0;
      for (let i = 0; i < nSamples; i++) {
        const sample = this.props.response.samples[i];

        for (let j = 0; j < sample.spanStats.length; j++) {
          const bucket = sample.spanStats[j];

          // compute x, y, width, and height of rendered span.
          const { x, y, width, height } = this.computeBucket(
            i,
            nSamples,
            bucket,
          );

          // compute color
          const color = [bucket.batchRequests / this.props.highestTemp, 0, 0];

          drawBucket(
            pixels,
            Math.ceil(x),
            Math.ceil(y),
            Math.ceil(width),
            Math.ceil(height),
            color,
          );
        }
      }

      // blit
      this.ctx.putImageData(imageData, 0, 0);
      // console.log("render time: ", window.performance.now() - startTime);

      // render y axis
      // choose 10 values to display

      this.ctx.fillStyle = "white";
      this.ctx.font = "12px sans-serif";
      let labelCount = 0;
      // const nSkip = 1;
      for (const [key, yOffset] of Object.entries(this.props.yOffsetForKey)) {
        labelCount++;
        // if (labelCount % nSkip === 0) {
        this.ctx.fillText(
          key,
          YAxisLabelPadding,
          yOffset * this.yZoomFactor + this.yPanOffset,
        );
        // }
      }

      // render x axis
      for (let i = 0; i < nSamples; i++) {
        const sample = this.props.response.samples[i];

        const timeString = new Date(
          sample.sampleTime.wallTime / 1e6,
        ).toUTCString();
        const x =
          YAxisLabelPadding +
          this.xPanOffset +
          (i * CanvasWidth * this.xZoomFactor) / nSamples;

        const y = CanvasHeight - XAxisLabelPadding;
        this.ctx.fillText(timeString, x, y);
      }
    }); // end RAF
  };

  computeBucket(sampleIndex: number, nSamples: number, bucket: SpanStatistics) {
    const x =
      YAxisLabelPadding +
      this.xPanOffset +
      (sampleIndex * CanvasWidth * this.xZoomFactor) / nSamples;
    const y =
      this.props.yOffsetForKey[bucket.span.startKey] * this.yZoomFactor +
      this.yPanOffset;

    const width =
      ((CanvasWidth - YAxisLabelPadding) * this.xZoomFactor) / nSamples;
    const height =
      this.props.yOffsetForKey[bucket.span.endKey] * this.yZoomFactor -
      y +
      this.yPanOffset;

    return {
      x,
      y,
      width,
      height,
    };
  }

  componentDidMount() {
    this.ctx = this.canvasRef.current.getContext("2d");
  }

  componentDidUpdate() {
    // console.warn("component update");
    this.renderKeyVisualizer();
  }

  handleCanvasScroll = (e: any) => {
    if (!this.zoomHandlerThrottled) {
      this.zoomHandlerThrottled = throttle(e => {
        // normalize value and negate so that "scrolling up" zooms in
        const deltaY = -e.deltaY / 100;

        this.yZoomFactor += deltaY;
        this.xZoomFactor += deltaY;

        // clamp zoom factor between 1 and 10
        this.yZoomFactor = Math.max(1, Math.min(20, this.yZoomFactor));
        this.xZoomFactor = Math.max(1, Math.min(20, this.xZoomFactor));

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

  handleCanvasPan = (e: any) => {
    if (!this.panHandlerThrottled) {
      this.panHandlerThrottled = throttle(e => {
        this.xPanOffset += e.movementX;
        this.yPanOffset += e.movementY;

        this.yPanOffset = Math.min(0, this.yPanOffset);
        this.xPanOffset = Math.min(0, this.xPanOffset);

        if (this.xPanOffset < 0) {
          let topRight = this.xPanOffset + CanvasWidth * this.xZoomFactor;

          // top right can never be less than CanvasWidth
          topRight = Math.max(CanvasWidth, topRight);

          // convert back to top left
          this.xPanOffset = topRight - CanvasWidth * this.xZoomFactor;
        }

        if (this.yPanOffset < 0) {
          let bottomLeft = this.yPanOffset + CanvasHeight * this.yZoomFactor;
          bottomLeft = Math.max(CanvasHeight, bottomLeft);
          this.yPanOffset = bottomLeft - CanvasHeight * this.yZoomFactor;
        }

        this.renderKeyVisualizer();
      }, 1000 / 60);
    }

    this.panHandlerThrottled(e);
  };

  handleCanvasHover = (e: any) => {
    if (!this.hoverHandlerThrottled) {
      this.hoverHandlerThrottled = throttle(e => {
        const mouseX = e.nativeEvent.offsetX;
        const mouseY = e.nativeEvent.offsetY;
        const nSamples = this.props.response.samples.length;
        // label this for loop so we can break from it.
        // I thought this would need to be implemented with some sort of O(1) lookup
        // or a binary partitioning scheme, but a naive `for` loop seems to be fast enough...
        iterate_samples: for (let i = 0; i < nSamples; i++) {
          const sample = this.props.response.samples[i];

          for (let j = 0; j < sample.spanStats.length; j++) {
            const bucket = sample.spanStats[j];

            const { x, y, width, height } = this.computeBucket(
              i,
              nSamples,
              bucket,
            );

            if (
              mouseX >= x &&
              mouseX <= x + width &&
              mouseY >= y &&
              mouseY <= y + height
            ) {
              const timeString = new Date(sample.sampleTime.wallTime / 1e6).toUTCString();
              this.props.setTooltipDetails(mouseX, mouseY, timeString, bucket);
              break iterate_samples;
            }
          }
        }
      }, 50);
    }

    this.hoverHandlerThrottled(e);
  };

  render() {
    return (
      <canvas
        onWheel={e => {
          e.persist();
          this.handleCanvasScroll(e);
        }}
        onMouseDown={() => {
          this.isPanning = true;
          this.props.setShowTooltip(false);
        }}
        onMouseUp={() => {
          this.isPanning = false;
          this.props.setShowTooltip(true);
        }}
        onMouseMove={e => {
          e.persist();

          if (this.isPanning) {
            this.handleCanvasPan(e);
          } else {
            this.handleCanvasHover(e);
          }
        }}
        width={CanvasWidth}
        height={CanvasHeight}
        ref={this.canvasRef}
      />
    );
  }
}
