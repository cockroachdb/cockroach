/// <reference path="../../external/bytebuffer.d.ts"/>

/** This file was automatically generated using protobuf2typescript. Then the decode
 * functions were uncommented, a reference to bytebuffer was added, and the JSON
 * types and constructors were added.
 */

declare module Proto2TypeScript {
  interface ProtoBufModel {
    toArrayBuffer(): ArrayBuffer;
    toBuffer(): NodeBuffer;
    encode(): ByteBuffer;
    toBase64(): string;
    toString(): string;
  }

  export interface ProtoBufBuilder {
    gogoproto: gogoprotoBuilder;
    cockroach: cockroachBuilder;
  }
}

declare module Proto2TypeScript {

  export interface gogoproto extends ProtoBufModel {
  }

  export interface gogoprotoJSON {
  }
  
  export interface gogoprotoBuilder {
    new(): gogoproto;
    new(gogoproto: gogoprotoJSON): gogoproto;
    decode(buffer: ArrayBuffer) : gogoproto;
    decode(buffer: NodeBuffer) : gogoproto;
    //decode(buffer: ByteArrayBuffer) : gogoproto;
    decode64(buffer: string) : gogoproto;
  }  
}

declare module Proto2TypeScript {

  export interface cockroach extends ProtoBufModel {
  }

  export interface cockroachJSON {
  }
  
  export interface cockroachBuilder {
    new(): cockroach;
    new(cockroach: cockroachJSON): cockroach;
    decode(buffer: ArrayBuffer) : cockroach;
    decode(buffer: NodeBuffer) : cockroach;
    //decode(buffer: ByteArrayBuffer) : cockroach;
    decode64(buffer: string) : cockroach;
    sql: cockroach.sqlBuilder;
  }  
}

declare module Proto2TypeScript.cockroach {

  export interface sql extends ProtoBufModel {
  }

  export interface sqlJSON {
  }
  
  export interface sqlBuilder {
    new(): sql;
    new(sql: sqlJSON): sql;
    decode(buffer: ArrayBuffer) : sql;
    decode(buffer: NodeBuffer) : sql;
    //decode(buffer: ByteArrayBuffer) : sql;
    decode64(buffer: string) : sql;
    driver: sql.driverBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql {

  export interface driver extends ProtoBufModel {
  }

  export interface driverJSON {
  }

  export interface driverBuilder {
    new(): driver;
    new(driver: driverJSON): driver;
    decode(buffer: ArrayBuffer) : driver;
    decode(buffer: NodeBuffer) : driver;
    //decode(buffer: ByteArrayBuffer) : driver;
    decode64(buffer: string) : driver;
    Datum: driver.DatumBuilder;
    Request: driver.RequestBuilder;
    Response: driver.ResponseBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver {

  export interface Datum extends ProtoBufModel {
    bool_val?: boolean;
    getBoolVal() : boolean;
    setBoolVal(boolVal : boolean): void;
    int_val?: number;
    getIntVal() : number;
    setIntVal(intVal : number): void;
    float_val?: number;
    getFloatVal() : number;
    setFloatVal(floatVal : number): void;
    bytes_val?: ByteBuffer;
    getBytesVal() : ByteBuffer;
    setBytesVal(bytesVal : ByteBuffer): void;
    string_val?: string;
    getStringVal() : string;
    setStringVal(stringVal : string): void;
    date_val?: number;
    getDateVal() : number;
    setDateVal(dateVal : number): void;
    time_val?: Datum.Timestamp;
    getTimeVal() : Datum.Timestamp;
    setTimeVal(timeVal : Datum.Timestamp): void;
    interval_val?: number;
    getIntervalVal() : number;
    setIntervalVal(intervalVal : number): void;
  }

  export interface DatumJSON {
    bool_val?: boolean;
    int_val?: number;
    float_val?: number;
    bytes_val?: ByteBuffer;
    string_val?: string;
    date_val?: number;
    time_val?: Datum.TimestampJSON;
    interval_val?: number;
  }
  
  export interface DatumBuilder {
    new(): Datum;
    new(datum: DatumJSON): Datum;
    decode(buffer: ArrayBuffer) : Datum;
    decode(buffer: NodeBuffer) : Datum;
    //decode(buffer: ByteArrayBuffer) : Datum;
    decode64(buffer: string) : Datum;
    Timestamp: Datum.TimestampBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Datum {

  export interface Timestamp extends ProtoBufModel {
    sec?: number;
    getSec() : number;
    setSec(sec : number): void;
    nsec?: number;
    getNsec() : number;
    setNsec(nsec : number): void;
  }

  export interface TimestampJSON {
    sec?: number;
    nsec?: number;
  }
  
  export interface TimestampBuilder {
    new(): Timestamp;
    new(timestamp: TimestampJSON): Timestamp;
    decode(buffer: ArrayBuffer) : Timestamp;
    decode(buffer: NodeBuffer) : Timestamp;
    //decode(buffer: ByteArrayBuffer) : Timestamp;
    decode64(buffer: string) : Timestamp;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver {

  export interface Request extends ProtoBufModel {
    user?: string;
    getUser() : string;
    setUser(user : string): void;
    session?: ByteBuffer;
    getSession() : ByteBuffer;
    setSession(session : ByteBuffer): void;
    sql?: string;
    getSql() : string;
    setSql(sql : string): void;
    params: Datum[];
    getParams() : Datum[];
    setParams(params : Datum[]): void;
  }

  export interface RequestJSON {
    user?: string;
    session?: ByteBuffer;
    sql?: string;
    params?: DatumJSON[];
  }
  
  export interface RequestBuilder {
    new(): Request;
    new(request: RequestJSON): Request;
    decode(buffer: ArrayBuffer) : Request;
    decode(buffer: NodeBuffer) : Request;
    //decode(buffer: ByteArrayBuffer) : Request;
    decode64(buffer: string) : Request;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver {

  export interface Response extends ProtoBufModel {
    session?: ByteBuffer;
    getSession() : ByteBuffer;
    setSession(session : ByteBuffer): void;
    results: Response.Result[];
    getResults() : Response.Result[];
    setResults(results : Response.Result[]): void;
  }

  export interface ResponseJSON {
    session?: ByteBuffer;
    results?: Response.ResultJSON[];
  }
  
  export interface ResponseBuilder {
    new(): Response;
    new(response: ResponseJSON): Response;
    decode(buffer: ArrayBuffer) : Response;
    decode(buffer: NodeBuffer) : Response;
    //decode(buffer: ByteArrayBuffer) : Response;
    decode64(buffer: string) : Response;
    Result: Response.ResultBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Response {

  export interface Result extends ProtoBufModel {
    error?: string;
    getError() : string;
    setError(error : string): void;
    ddl?: Result.DDL;
    getDdl() : Result.DDL;
    setDdl(ddl : Result.DDL): void;
    rows_affected?: number;
    getRowsAffected() : number;
    setRowsAffected(rowsAffected : number): void;
    rows?: Result.Rows;
    getRows() : Result.Rows;
    setRows(rows : Result.Rows): void;
  }

  export interface ResultJSON {
    error?: string;
    ddl?: Result.DDLJSON;
    rows_affected?: number;
    rows?: Result.RowsJSON;
  }
  
  export interface ResultBuilder {
    new(): Result;
    new(result: ResultJSON): Result;
    decode(buffer: ArrayBuffer) : Result;
    decode(buffer: NodeBuffer) : Result;
    //decode(buffer: ByteArrayBuffer) : Result;
    decode64(buffer: string) : Result;
    DDL: Result.DDLBuilder;
    Rows: Result.RowsBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Response.Result {

  export interface DDL extends ProtoBufModel {
  }

  export interface DDLJSON {
  }

  export interface DDLBuilder {
    new(): DDL;
    new(ddl: DDLJSON): DDL;
    decode(buffer: ArrayBuffer) : DDL;
    decode(buffer: NodeBuffer) : DDL;
    //decode(buffer: ByteArrayBuffer) : DDL;
    decode64(buffer: string) : DDL;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Response.Result {

  export interface Rows extends ProtoBufModel {
    columns: Rows.Column[];
    getColumns() : Rows.Column[];
    setColumns(columns : Rows.Column[]): void;
    rows: Rows.Row[];
    getRows() : Rows.Row[];
    setRows(rows : Rows.Row[]): void;
  }

  export interface RowsJSON {
    columns?: Rows.ColumnJSON[];
    rows?: Rows.RowJSON[];
  }

  export interface RowsBuilder {
    new(): Rows;
    new(rows: RowsJSON): Rows;
    decode(buffer: ArrayBuffer) : Rows;
    decode(buffer: NodeBuffer) : Rows;
    //decode(buffer: ByteArrayBuffer) : Rows;
    decode64(buffer: string) : Rows;
    Row: Rows.RowBuilder;
    Column: Rows.ColumnBuilder;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Response.Result.Rows {

  export interface Row extends ProtoBufModel {
    values: Datum[];
    getValues() : Datum[];
    setValues(values : Datum[]): void;
  }

  export interface RowJSON {
    values?: DatumJSON[];
  }
  
  export interface RowBuilder {
    new(): Row;
    new(row: RowJSON): Row;
    decode(buffer: ArrayBuffer) : Row;
    decode(buffer: NodeBuffer) : Row;
    //decode(buffer: ByteArrayBuffer) : Row;
    decode64(buffer: string) : Row;
  }  
}

declare module Proto2TypeScript.cockroach.sql.driver.Response.Result.Rows {

  export interface Column extends ProtoBufModel {
    name?: string;
    getName() : string;
    setName(name : string): void;
    typ?: Datum;
    getTyp() : Datum;
    setTyp(typ : Datum): void;
  }

  export interface ColumnJSON {
    name?: string;
    typ?: DatumJSON;
  }
  
  export interface ColumnBuilder {
    new(): Column;
    new(column: ColumnJSON): Column;
    decode(buffer: ArrayBuffer) : Column;
    decode(buffer: NodeBuffer) : Column;
    //decode(buffer: ByteArrayBuffer) : Column;
    decode64(buffer: string) : Column;
  }  
}
