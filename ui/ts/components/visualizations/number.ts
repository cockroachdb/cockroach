/// <reference path="../../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../../typings/browser.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Visualizations {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  interface NumberVisualizationData {
    value: number;
  }

  export interface NumberVisualizationConfig {
    format?: string; // TODO: better automatic formatting
    formatFn?: (n: number) => string;
    zoom?: string; // TODO: compute fontsize/zoom automatically
    data: NumberVisualizationData;
  }

  export module NumberVisualization {
    export function controller(): void {}

    export function view(ctrl: any, info: NumberVisualizationConfig): MithrilVirtualElement {
      let formatFn: (n: number) => string = info.formatFn || d3.format(info.format || ".3s");

      return m(
        ".visualization",
        m(".number", {style: "zoom:" + (info.zoom || "100%") + ";"}, formatFn(info.data.value))
      );
    }
  }
}
