// source: pages/monitor.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  "use strict";

  /**
   * Monitor is the view for exploring cluster status.
   */
  export module Monitor {
    export module Page {
      export function controller(): void { }
      export function view(): _mithril.MithrilVirtualElement {
        return m("h3", "Monitor Placeholder");
      }
    }
  }
}
