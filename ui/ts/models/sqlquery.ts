/// <reference path="../../bower_components/mithriljs/mithril.d.ts" />
/// <reference path="../../typings/tsd.d.ts" />
/// <reference path="../models/proto.ts" />

module Models {
  "use strict";
  export module SQLQuery {
    import Request = Models.Proto.Request;
    import Response = Models.Proto.Response;

    export function runQuery(q: string): _mithril.MithrilPromise<Response> {

      let data: Request = {user: "root", sql: q};

      let xhrConfig = function(xhr: XMLHttpRequest): XMLHttpRequest {
        xhr.setRequestHeader("Content-Type", "application/x-protobuf");
        xhr.setRequestHeader("Accept", "application/json");

        /**
         * TODO: receive data as protobuf instead of JSON
         *
         * Mithril doesn't support sending/receiving binary data, but xhr
         * does. We just need the following lines to send/receive binary
         * data at the xhr level, but mithril would need to be modified to
         * support it, or we'd need a different ajax library.
         *
         *   xhr.setRequestHeader("Accept", "application/x-protobuf");
         *   xhr.responseType = "arraybuffer";
         *
         * Once this was enabled, we could use proto2typescript to generate
         * typescript definitions of protobufs.
         * 
         */
        return xhr;
      };

      return m.request({
        url: "/sql/Execute",
        config: xhrConfig,
        method: "POST",
        data: data,
      });
    }

  }
}
