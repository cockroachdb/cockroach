import React from "react"
import {SpanStatistics} from "oss/src/views/keyVisualizer/interfaces";

export interface SpanDetailTooltipProps {
  x: number;
  y: number;
  time: string;
  spanStats: SpanStatistics;
}

export const SpanDetailTooltip: React.FunctionComponent<SpanDetailTooltipProps> = (
  props
) => {
  return (
    <div
      style={{
        fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif",
        position: "absolute",
        left: `${props.x + 60}px`,
        top: `${props.y + 30}px`,
        background: "white",
        padding: "20px",
        borderRadius: "4px",
      }}
    >
      <p>time: {props.time}</p>
      <p>start key: {props.spanStats?.span.startKey}</p>
      <p>end key: {props.spanStats?.span.endKey}</p>
      <p>batch reqs: {props.spanStats?.batchRequests}</p>
    </div>
  );
};
