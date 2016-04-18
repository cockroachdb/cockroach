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
    tooltip?: string;
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

      let topContent: _mithril.MithrilVirtualElement[] = [];
      if (info.tooltip || (info.warning && info.warning())) {
        topContent.push(m(".viz-info-icon", icon));
      }
      if (info.tooltip) {
        topContent.push(m.component(Components.Tooltip, {
          tooltipClass: ".viz-tooltip",
          title: info.title,
          content: info.tooltip,
        }));
      }

      return m(
        ".visualization-wrapper" + warningClass,
        [
          m(".viz-top", topContent),
          info.virtualVisualizationElement,
          m(".viz-bottom", m(".viz-title", info.title)),
        ]
      );
    }
  }

}
