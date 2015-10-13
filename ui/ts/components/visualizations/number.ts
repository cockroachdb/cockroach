/// <reference path="../../external/mithril/mithril.d.ts" />
/// <reference path="../../../typings/d3/d3.d.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Visualizations {
  "use strict";

  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  let formatFn: (n: number) => string;

  export module NumberVisualization {
    export function controller(): any {
      return {};
    }

    export function view(ctrl: any, info: VisualizationInfo): MithrilVirtualElement {

      formatFn = info.visualizationArguments.formatFn || d3.format(info.visualizationArguments.format || ".3s");

      let data: VisualizationData = _.isArray(info.data) ? info.data[0] : info.data;

      return m(
        ".visualization",
        {style: "width:" + info.width.toString() + "px; height:" + info.height.toString() + "px; " +
          "line-height:" + info.height.toString() + "px; " },
        [
          m(".number", {style: "zoom:" + (info.visualizationArguments.zoom || "100%") + ";"}, formatFn(data.value))
        ]);
    }
  }
}
