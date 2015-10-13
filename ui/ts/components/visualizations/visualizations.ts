/// <reference path="./bar.ts" />
/// <reference path="./bullet.ts" />
/// <reference path="./line.ts" />
/// <reference path="./number.ts" />
/// <reference path="./pie.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Visualizations {
  "use strict";
  import MithrilVirtualElement = _mithril.MithrilVirtualElement;

  export interface VisualizationData {
    key?: string;
    value: number;
  }

  export interface VisualizationWrapperInfo {
    title?: string;
    virtualVisualizationElement: MithrilVirtualElement;
  }

  export module VisualizationWrapper {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    export function controller(): void {
    }

    export function view(ctrl: any, info: VisualizationWrapperInfo): MithrilVirtualElement {

      return m(
        ".visualization-wrapper",
        [
          // TODO: pass in and display info icon tooltip
          m(".viz-top", m(".viz-info-icon", m(".icon-cockroach-17"))), // Icon Cockroach 17 is the info icon.
         info.virtualVisualizationElement,
          m(".viz-bottom", m(".viz-title", info.title))
        ]
      );
    }
  }

}
