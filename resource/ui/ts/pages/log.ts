// source: pages/log.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
/// <reference path="../models/log.ts" />

// Author: Bram Gruneir (bram+code@cockroachlabs.com)

/**
 * AdminViews is the primary module for Cockroaches administrative web
 * interface.
 */
module AdminViews {
    /**
     * Log is the view for exploring the logs from nodes.
     */
    export module Log {
        var entries = new Models.Log.Entries();

        /**
         * Page displays log entries from the current node.
         */
        export module Page {
            class Controller {
                private static _queryEveryMS = 10000;
                private _interval: number;

                private _Refresh(): void {
                    entries.Refresh();
                }

                public constructor() {
                    this._Refresh();
                    this._interval = setInterval(() => this._Refresh(), Controller._queryEveryMS);
                }

                public onunload() {
                    clearInterval(this._interval);
                }
            }

            export function controller(): Controller {
                return new Controller();
            }

            export function view(ctrl: Controller) {
                return entries.EntryRows();
            }
        }
    }
}
