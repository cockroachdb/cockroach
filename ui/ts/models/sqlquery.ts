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
         * Currently there are two separate protobuf issues:
         *
         * 1) Mithril doesn't support sending/receiving binary data, but xhr
         * does. We just need the following lines to send/receive binary
         * data at the xhr level, but mithril would need to be modified to
         * support it, or we'd need a different ajax library.
         *
         *   xhr.setRequestHeader("Accept", "application/x-protobuf");
         *   xhr.responseType = "arraybuffer";
         *
         *  2) proto2typescript (https://github.com/SINTEF-9012/Proto2TypeScript
         *  or the fork github.com/aliok/Proto2TypeScript) doesn't handle the
         *  proto oneof fields the way Go serializes them by default.
         *  The former ellides the oneof field and just includes the subfield
         *  direclty, whereas the latter creates an extra key and nests the
         *  subfield under that key. For example:
         *
         *  From wire.proto:
         *
         *   message Datum {
         *    oneof payload {
         *      bool bool_val = 1;
         *      int64 int_val = 2;
         *      ...
         *    }
         *  }
         *
         * Typescript definition from proto2typescript:
         *
         * interface Datum {
         *   bool_val?: boolean;
         *   int_val?: number;
         *   ...
         * }
         *
         * GO serialized JSON:
         *
         * {
         *    Datum: {
         *      Payload: {
         *        BoolVal: ...
         *      }
         *    }
         *  }
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
