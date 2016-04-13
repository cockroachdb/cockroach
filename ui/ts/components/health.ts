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

    export function view(ctrl: any): _mithril.MithrilVirtualElement {
      if (Health.healthy) {
        return m("div", [
          m("span.good", "Good"),
          m("span.health-icon.icon-check-circle"),
        ]);
      } else {
        return m("div", [
          m("span.unreachable-text", "Can't reach node "),
          m("span.health-icon.icon-stop-sign"),
        ]);
      }
    }
  }
}
