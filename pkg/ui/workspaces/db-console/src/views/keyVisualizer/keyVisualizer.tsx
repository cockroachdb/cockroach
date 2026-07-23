// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import throttle from "lodash/throttle";
import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

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

const KeyVisualizer: React.FC<KeyVisualizerProps> = props => {
  const { samples, keys, yOffsetsForKey, hottestBucket } = props;

  const canvasRef = useRef<HTMLCanvasElement>(null);
  const ctxRef = useRef<CanvasRenderingContext2D>(null);

  // Mutable pan/zoom state — these change on every scroll frame and must
  // not trigger re-renders, so they live in refs rather than useState.
  const xPanOffset = useRef(0);
  const yPanOffset = useRef(0);
  const xZoomFactor = useRef(1);
  const yZoomFactor = useRef(1);

  const [showTooltip, setShowTooltip] = useState(false);
  const [showSpanBoundaries, setShowSpanBoundaries] = useState(true);
  const [tooltipState, setTooltipState] = useState<TooltipProps>({
    x: 0,
    y: 0,
    startKey: "",
    endKey: "",
    requests: 0,
    epochTime: 0,
  });

  // Track showSpanBoundaries in a ref so the canvas render callback
  // (called from throttled handlers) always sees the latest value.
  const showSpanBoundariesRef = useRef(showSpanBoundaries);
  showSpanBoundariesRef.current = showSpanBoundaries;

  // Keep a ref to the latest props so throttled callbacks can access
  // fresh data without needing to be recreated on every prop change.
  const propsRef = useRef(props);
  propsRef.current = props;

  // Initialize canvas context on mount.
  useEffect(() => {
    ctxRef.current = canvasRef.current.getContext("2d");
  }, []);

  const computeBucketDimensions = useCallback(
    (sampleIndex: number, bucket: SampleBucket) => {
      const p = propsRef.current;
      const startKey = p.keys[bucket.startKeyHex];
      const endKey = p.keys[bucket.endKeyHex];
      const nSamples = p.samples.length;

      const x =
        YAxisLabelPadding +
        xPanOffset.current +
        (sampleIndex * CanvasWidth * xZoomFactor.current) / nSamples;

      const y =
        p.yOffsetsForKey[startKey] * yZoomFactor.current + yPanOffset.current;

      const width =
        ((CanvasWidth - YAxisLabelPadding) * xZoomFactor.current) / nSamples;
      const height =
        p.yOffsetsForKey[endKey] * yZoomFactor.current - y + yPanOffset.current;

      return { x, y, width, height };
    },
    [],
  );

  const renderCanvas = useCallback(() => {
    const ctx = ctxRef.current;
    if (!ctx) return;
    const p = propsRef.current;

    requestAnimationFrame(() => {
      // clear
      ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
      ctx.fillStyle = "black";
      ctx.fillRect(0, 0, ctx.canvas.width, ctx.canvas.height);

      const imageData = ctx.getImageData(
        0,
        0,
        ctx.canvas.width,
        ctx.canvas.height,
      );
      const pixels = imageData.data;

      // render samples
      const nSamples = p.samples.length;
      for (let i = 0; i < nSamples; i++) {
        const sample = p.samples[i];

        for (let j = 0; j < sample.buckets.length; j++) {
          const bucket = sample.buckets[j];
          const dim = computeBucketDimensions(i, bucket);
          const { x, y, width, height } = dim;

          const color = [
            Math.log(Math.max(getRequestsAsNumber(bucket.requests), 1)) /
              Math.log(p.hottestBucket),
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
            showSpanBoundariesRef.current,
          );
        }
      }

      // blit
      ctx.putImageData(imageData, 0, 0);

      // render y axis
      ctx.fillStyle = "white";
      ctx.font = "12px sans-serif";

      const yAxisLabels = filterAxisLabels(
        xZoomFactor.current,
        yPanOffset.current,
        p.yOffsetsForKey,
        MaxLabelsYAxis,
        CanvasHeight,
      );

      for (const [key, yOff] of Object.entries(yAxisLabels)) {
        ctx.fillText(
          key,
          YAxisLabelPadding,
          yOff * yZoomFactor.current + yPanOffset.current,
        );
      }

      // render x axis
      const xOffsetForSampleTime = p.samples.reduce(
        (acc, sample, index) => {
          const wallTimeMs = sample.timestamp.seconds.toNumber() * 1e3;
          const timeString = new Date(wallTimeMs).toISOString();
          const offset = (index * CanvasWidth) / p.samples.length;
          acc[timeString] = offset;
          return acc;
        },
        {} as Record<string, number>,
      );

      const xAxisLabels = filterAxisLabels(
        xZoomFactor.current,
        xPanOffset.current,
        xOffsetForSampleTime,
        MaxLabelsXAxis,
        CanvasWidth,
      );

      for (const [timestring, xOff] of Object.entries(xAxisLabels)) {
        const [s1, s2] = timestring.split("T");
        ctx.fillText(
          s1,
          YAxisLabelPadding + xPanOffset.current + xOff * xZoomFactor.current,
          CanvasHeight - XAxisLabelPadding,
        );
        ctx.fillText(
          s2,
          YAxisLabelPadding + xPanOffset.current + xOff * xZoomFactor.current,
          CanvasHeight - 2.5 * XAxisLabelPadding,
        );
      }
    });
  }, [computeBucketDimensions]);

  // Re-render the canvas when props change (equivalent to componentDidUpdate
  // with prevProps !== this.props check — the effect runs only when the prop
  // values actually change).
  useEffect(() => {
    renderCanvas();
  }, [samples, keys, yOffsetsForKey, hottestBucket, renderCanvas]);

  const handleCanvasScroll = useMemo(
    () =>
      throttle((e: any) => {
        // normalize value and negate so that "scrolling up" zooms in
        const deltaY = -e.deltaY / 100;

        yZoomFactor.current += deltaY;
        xZoomFactor.current += deltaY;

        // clamp zoom factor between 1 and MaxZoom
        yZoomFactor.current = Math.max(
          1,
          Math.min(MaxZoom, yZoomFactor.current),
        );
        xZoomFactor.current = Math.max(
          1,
          Math.min(MaxZoom, xZoomFactor.current),
        );

        // find mouse coordinates in terms of current window
        const windowPercentageX = e.nativeEvent.offsetX / CanvasWidth;
        const windowPercentageY = e.nativeEvent.offsetY / CanvasHeight;

        const z =
          xZoomFactor.current === 1
            ? 0
            : (xZoomFactor.current - 1) / (MaxZoom - 1);
        xPanOffset.current = windowPercentageX * CanvasWidth * MaxZoom * z * -1;
        yPanOffset.current =
          windowPercentageY * CanvasHeight * MaxZoom * z * -1;

        // if zoomed out, reset pan
        if (yZoomFactor.current === 1 && xZoomFactor.current === 1) {
          xPanOffset.current = 0;
          yPanOffset.current = 0;
        }

        renderCanvas();
      }, 1000 / 60),
    [renderCanvas],
  );

  const handleCanvasHover = useMemo(
    () =>
      throttle((e: any) => {
        const rect = canvasRef.current.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;
        const p = propsRef.current;
        const nSamples = p.samples.length;

        // label this for loop so we can break from it.
        iterate_samples: for (let i = 0; i < nSamples; i++) {
          const sample = p.samples[i];

          for (let j = 0; j < sample.buckets.length; j++) {
            const bucket = sample.buckets[j];
            const { x, y, width, height } = computeBucketDimensions(i, bucket);

            if (
              mouseX >= x &&
              mouseX <= x + width &&
              mouseY >= y &&
              mouseY <= y + height
            ) {
              setTooltipState({
                x: x + width + 20,
                y: y,
                startKey: p.keys[bucket.startKeyHex],
                endKey: p.keys[bucket.endKeyHex],
                requests: getRequestsAsNumber(bucket.requests),
                epochTime: sample.timestamp.seconds.toNumber(),
              });
              break iterate_samples;
            }
          } // end iterate buckets
        } // end iterate_samples
      }, 50),
    [computeBucketDimensions],
  );

  const toggleShowSpanBoundaries = useCallback(() => {
    setShowSpanBoundaries(prev => !prev);
  }, []);

  // Re-render canvas when showSpanBoundaries changes (replaces the
  // setState callback pattern from the class component).
  useEffect(() => {
    renderCanvas();
  }, [showSpanBoundaries, renderCanvas]);

  return (
    <>
      <canvas
        onWheel={e => {
          e.persist();
          handleCanvasScroll(e);
        }}
        onMouseEnter={() => setShowTooltip(true)}
        onMouseOut={() => setShowTooltip(false)}
        onMouseMove={e => {
          e.persist();
          handleCanvasHover(e);
        }}
        width={CanvasWidth}
        height={CanvasHeight}
        ref={canvasRef}
      />
      {showTooltip && <BucketToolTip {...tooltipState} />}
      <div>
        <label>
          <input
            type="checkbox"
            checked={showSpanBoundaries}
            onChange={toggleShowSpanBoundaries}
          />{" "}
          Show span boundaries
        </label>
      </div>
    </>
  );
};

export default KeyVisualizer;
