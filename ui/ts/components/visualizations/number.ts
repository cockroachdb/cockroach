/// <reference path="../../external/mithril/mithril.d.ts" />
/// <reference path="../../../typings/d3/d3.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Visualizations {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  let formatFn: (n: number) => string;

  interface NumberVisualizationData {
    value: number;
  }

  interface NumberVisualizationConfig {
    format?: string; // TODO: better automatic formatting
    formatFn?: (n: number) => string;
    zoom?: string; // TODO: compute fontsize/zoom automatically
    data: NumberVisualizationData;
  }

  export module NumberVisualization {
    export function controller(): any {
    }

    export function view(ctrl: any, info: NumberVisualizationConfig): MithrilVirtualElement {

      formatFn = info.formatFn || d3.format(info.format || ".3s");

      let data: VisualizationData = _.isArray(info.data) ? info.data[0] : info.data;

      return m(
        ".visualization",
        m(".number", {style: "zoom:" + (info.zoom || "100%") + ";"}, formatFn(data.value))
      );
    }
  }
}
