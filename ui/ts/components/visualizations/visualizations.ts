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
    value: number;
  }

  export interface VisualizationWrapperInfo {
    title: string;
    virtualVisualizationElement: MithrilVirtualElement;
    visualizationArguments?: any;
    warning?: () => string;
  }

  export module VisualizationWrapper {
    import MithrilVirtualElement = _mithril.MithrilVirtualElement;

    export function controller(): void {}

    export function view(ctrl: any, info: VisualizationWrapperInfo): MithrilVirtualElement {
      let icon: MithrilVirtualElement = m(".icon-info");
      let warningClass = "";
      if (info.warning && info.warning()) {
        icon = m(".icon-warning", {
                 title: info.warning(),
        });
        warningClass = " .viz-faded";
      }

      return m(
        ".visualization-wrapper" + warningClass,
        [
          // TODO: pass in and display info icon tooltip
          m(".viz-top", m(".viz-info-icon", icon)),
          info.virtualVisualizationElement,
          m(".viz-bottom", m(".viz-title", info.title)),
        ]
      );
    }
  }

}
