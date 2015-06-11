// source: util/query.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Matt Tracy (matt@cockroachlabs.com)

module Utils {
    /**
     * Http exports static http methods designed to work with standard
     * Cockroach HTTP endpoints.
     */
    export module Http {
        import promise = _mithril.MithrilPromise;

        /**
         * Get sends an GET request to the given relative URL, and returns
         * a mithril promise for the results of the request.
         */
        export function Get(url:string) {
            return m.request({url:url, method:"GET", extract:nonJsonErrors});
        }

        /**
         * Post sends an POST request to the given relative URL, and returns
         * a mithril promise for the results of the request. Provided data is
         * encoded as JSON before being sent as the body of the request.
         */
        export function Post(url:string, data:any) {
            return m.request({url:url, method:"POST", extract:nonJsonErrors, data:data});
        }

        /**
         * nonJsonErrors ensures that error messages returned from the server
         * are parseable as JSON strings.
         */
        function nonJsonErrors(xhr: XMLHttpRequest, opts: _mithril.MithrilXHROptions):string {
            return xhr.status > 200 ? JSON.stringify(xhr.responseText) : xhr.responseText;
        }
    }
}
