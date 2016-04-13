// source: components/health.ts
/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/browser.d.ts" />
/// <reference path="../util/property.ts" />
/// <reference path="../models/health.ts" />

// Author: Max Lang (max@cockroachlabs.com)

module Components {
  "use strict";

  /**
   * Health returns the health status as an icon
   */
  export module Health {
    import Health = Models.Health;

    export function controller(): any {
    }

    // TODO: add back refreshing indicators and some indicator of last refresh, and last _successful_ refresh
    // They are currently removed because the refresh interval for the health indicator runs at a different
    // time/frequency from the chart refreshes, so when the "refreshing..." text flashes it's confusing.
    // Once we coordinate refreshing globally across the UI (as mentioned in the controller) this can be re-added.
    export function view(ctrl: any): _mithril.MithrilVirtualElement {
      if (Health.healthy) {
        return m("div", [
          m("span.good", "Good"),
          m("span.health-icon.icon-check-circle" + (Health.refreshing ? ".refreshing" : "")),
          // m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      } else {
        return m("div", [
          m("span.unreachable-text", "Can't reach node "),
          m("span.health-icon.icon-stop-sign" + (Health.refreshing ? ".refreshing" : "")),
          // m("span.refreshing-text", ctrl.refreshing ? " Refreshing..." : ""),
        ]);
      }
    }
  }
}
