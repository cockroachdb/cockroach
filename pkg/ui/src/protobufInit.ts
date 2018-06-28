/// <reference path="../node_modules/protobufjs/stub-node.d.ts" />

import * as protobuf from "protobufjs/minimal";
import Long from "long";

protobuf.util.Long = Long as any;
protobuf.configure();
