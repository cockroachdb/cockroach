// source: controllers/monitor.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />

// Author: Bram Gruneir (bram.gruneir@gmail.com)
// Author: Matt Tracy (matt@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
  /**
   * Monitor is the view for exploring cluster status.
   */
  export module Monitor {
      export module Page {
          export function controller() {}
          export function view() {
              return m("h3", "Monitor Placeholder");
          }
      }
  }
}
