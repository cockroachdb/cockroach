// Type definitions for ByteBuffer.js 2.3.1
// Project: https://github.com/dcodeIO/ByteBuffer.js
// From: https://github.com/aliok/Proto2TypeScript/blob/master/definitions/bytebuffer.d.ts

declare class ByteBuffer {
  constructor(capacity?:number, littleEndian?:boolean);

  static BIG_ENDIAN:boolean;
  static DEFAULT_CAPACITY:number;
  static LITTLE_ENDIAN:boolean;
//    static Long?:Long;
  static MAX_VARINT32_BYTES:number;
  static MAX_VARINT64_BYTES:number;
  static VERSION:string;

  array:ArrayBuffer;
  length:number;
  littleEndian:boolean;
  markedOffset:number;
  offset:number;
  view:DataView;

  static allocate(capacity?:number, littleEndian?:number):ByteBuffer;

  static calculateUTF8Char(charCode:number):number;

  static calculateUTF8String(str:string):number;

  static calculateVariant32(value:number):number;

  static calculateVariant64(value:number):number;

  static decode64(str:string, littleEndian?:boolean):ByteBuffer;

  static decodeHex(str:string, littleEndian?:boolean):ByteBuffer;

  static decodeUTF8Char(str:string, offset:number):{char:number; length:number};

  static encode64(bb:ByteBuffer):string;

  static encodeUTF8Char(charCode:number, dst:ByteBuffer, offset:number):number;

  static wrap(buffer:ArrayBuffer, enc?:string, littleEndian?:boolean):ByteBuffer;

  static wrap(buffer:string, enc?:string, littleEndian?:boolean):ByteBuffer;

  static zigZagDecode32(n:number):number;

//    static zigZagDecode64(n:number):Long;

  static zigZagEncode32(n:number):number;

//    static zigZagEncode64(n:number):Long;

  append(src:any, offset?:number):ByteBuffer;

  BE(bigEndian?:boolean):ByteBuffer;

  capacity():number;

  clone():ByteBuffer;

  compact():ByteBuffer;

  copy():ByteBuffer;

  destroy():ByteBuffer;

  ensureCapacity(capacity:number):ByteBuffer;

  flip():ByteBuffer;

  LE(littleEndian?:boolean):ByteBuffer;

  mark(offset?:number):ByteBuffer;

  prepend(src:any, offset?:number):ByteBuffer;

  printDebug(out?:(string)=>void):void;

  readByte(offset?:number):number;

  readCString(offset?:number):string;

  readDouble(offset?:number):number;

  readFloat(offset?:number):number;

  readFloat32(offset?:number):number;

  readFloat64(offset?:number):number;

  readInt(offset?:number):number;

  readInt8(offset?:number):number;

  readInt16(offset?:number):number;

  readInt32(offset?:number):number;

  readInt64(offset?:number):number;

  readJSON(offset?:number, parse?:(string)=>void):any;

//    readLong(offset?:number):Long;

  readLString(offset?:number):any;

  readShort(offset?:number):number;

  readUint8(offset?:number):number;

  readUint16(offset?:number):number;

  readUint32(offset?:number):number;

//    readUint64(offset?:number):Long;

  readUTF8String(chars:number, offset?:number):string;

  readUTF8StringBytes(length:number, offset?:number):string;

  readVarint(offset?:number):number;

  readVarint32(offset?:number):number;

//    readVarint64(offset?:number):Long;

  readVString(offset?:number):string;

  readZigZagVarint(offset?:number):number;

  readZigZagVarint32(offset?:number):number;

//    readZigZagVarint64(offset?:number):Long;

  remaining():number;

  reset():ByteBuffer;

  resize(capacity:number):ByteBuffer;

  reverse():ByteBuffer;

  slice(begin?:number, end?:number):ByteBuffer;

  toArrayBuffer(forceCopy?:boolean):ArrayBuffer;

  toBase64():string;

//    toBuffer():Buffer;

  toColumns(wrap?:number):string;

  toHex(debug?:boolean):string;

  toString(enc?:string):string;

  toUTF8():string;

  writeByte(value:number, offset?:number):ByteBuffer;

  writeCString(str:string, offset?:number):ByteBuffer;

  writeDouble(value:number, offset?:number):ByteBuffer;

  writeFloat(value:number, offset?:number):ByteBuffer;

  writeFloat32(value:number, offset?:number):ByteBuffer;

  writeFloat64(value:number, offset?:number):ByteBuffer;

  writeInt(value:number, offset?:number):ByteBuffer;

  writeInt8(value:number, offset?:number):ByteBuffer;

  writeInt16(value:number, offset?:number):ByteBuffer;

  writeInt32(value:number, offset?:number):ByteBuffer;

  writeInt64(value:number, offset?:number):ByteBuffer;

  writeJSON(data:any, offset?:number, stringify?:any):ByteBuffer;

//    writeLong(value:number, offset?:number):ByteBuffer;

  writeLString(str:string, offset?:number):ByteBuffer;

  writeShort(value:number, offset?:number):ByteBuffer;

  writeUint8(value:number, offset?:number):ByteBuffer;

  writeUint16(value:number, offset?:number):ByteBuffer;

  writeUint32(value:number, offset?:number):ByteBuffer;

  writeUint64(value:number, offset?:number):ByteBuffer;

  writeUTF8String(str:string, offset?:number):ByteBuffer;

  writeVarint(value:number, offset?:number):ByteBuffer;

  writeVarint32(value:number, offset?:number):ByteBuffer;

  writeVarint64(value:number, offset?:number):ByteBuffer;

  writeVString(str:string, offset?:number):ByteBuffer;

  writeZigZagVarint(value:number, offset?:number):ByteBuffer;

  writeZigZagVarint32(value:number, offset?:number):ByteBuffer;

  writeZigZagVarint64(value:number, offset?:number):ByteBuffer;
}
