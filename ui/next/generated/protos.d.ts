declare module cockroach {
	
	
	interface ProtoBufMapItem<KeyType, ValueType> {
		key : KeyType,
		value : ValueType
	}
	
	interface ProtoBufMap<KeyType, ValueType> {
		clear(): void;
		delete(key: KeyType): void;
		get(key: KeyType): ValueType;
		has(key: KeyType): boolean;
		set(key: KeyType, value: ValueType): void;
		forEach(fn: (value: ValueType, key?: KeyType) => void): void;
		size: number;
		map : { [key: string]: ProtoBufMapItem<KeyType, ValueType> }
	}
	
	export interface ProtoBufBuilder {
		util: utilBuilder;
		roachpb: roachpbBuilder;
		build: buildBuilder;
		server: serverBuilder;
		ts: tsBuilder;
		
}
}

declare module cockroach {
	
	export interface util {
	
		

}
	
		
export interface utilMessage extends util {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface utilBuilder {
	new(data?: util): utilMessage;
	decode(buffer: ArrayBuffer) : utilMessage;
	//decode(buffer: NodeBuffer) : utilMessage;
	//decode(buffer: ByteArrayBuffer) : utilMessage;
	decode(buffer: ByteBuffer) : utilMessage;
	decode64(buffer: string) : utilMessage;
	UnresolvedAddr: util.UnresolvedAddrBuilder;
	
}
	
}

declare module cockroach.util {
	
	export interface UnresolvedAddr {
	
		

network_field?: string;
		

getNetworkField?() : string;
		setNetworkField?(networkField : string): void;
		



address_field?: string;
		

getAddressField?() : string;
		setAddressField?(addressField : string): void;
		



}
	
		
export interface UnresolvedAddrMessage extends UnresolvedAddr {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface UnresolvedAddrBuilder {
	new(data?: UnresolvedAddr): UnresolvedAddrMessage;
	decode(buffer: ArrayBuffer) : UnresolvedAddrMessage;
	//decode(buffer: NodeBuffer) : UnresolvedAddrMessage;
	//decode(buffer: ByteArrayBuffer) : UnresolvedAddrMessage;
	decode(buffer: ByteBuffer) : UnresolvedAddrMessage;
	decode64(buffer: string) : UnresolvedAddrMessage;
	
}
	
}



declare module cockroach {
	
	export interface roachpb {
	
		

}
	
		
export interface roachpbMessage extends roachpb {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface roachpbBuilder {
	new(data?: roachpb): roachpbMessage;
	decode(buffer: ArrayBuffer) : roachpbMessage;
	//decode(buffer: NodeBuffer) : roachpbMessage;
	//decode(buffer: ByteArrayBuffer) : roachpbMessage;
	decode(buffer: ByteBuffer) : roachpbMessage;
	decode64(buffer: string) : roachpbMessage;
	Attributes: roachpb.AttributesBuilder;
	ReplicaDescriptor: roachpb.ReplicaDescriptorBuilder;
	RangeDescriptor: roachpb.RangeDescriptorBuilder;
	StoreCapacity: roachpb.StoreCapacityBuilder;
	NodeDescriptor: roachpb.NodeDescriptorBuilder;
	StoreDescriptor: roachpb.StoreDescriptorBuilder;
	
}
	
}

declare module cockroach.roachpb {
	
	export interface Attributes {
	
		

attrs?: string[];
		

getAttrs?() : string[];
		setAttrs?(attrs : string[]): void;
		



}
	
		
export interface AttributesMessage extends Attributes {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface AttributesBuilder {
	new(data?: Attributes): AttributesMessage;
	decode(buffer: ArrayBuffer) : AttributesMessage;
	//decode(buffer: NodeBuffer) : AttributesMessage;
	//decode(buffer: ByteArrayBuffer) : AttributesMessage;
	decode(buffer: ByteBuffer) : AttributesMessage;
	decode64(buffer: string) : AttributesMessage;
	
}
	
}


declare module cockroach.roachpb {
	
	export interface ReplicaDescriptor {
	
		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



store_id?: number;
		

getStoreId?() : number;
		setStoreId?(storeId : number): void;
		



replica_id?: number;
		

getReplicaId?() : number;
		setReplicaId?(replicaId : number): void;
		



}
	
		
export interface ReplicaDescriptorMessage extends ReplicaDescriptor {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface ReplicaDescriptorBuilder {
	new(data?: ReplicaDescriptor): ReplicaDescriptorMessage;
	decode(buffer: ArrayBuffer) : ReplicaDescriptorMessage;
	//decode(buffer: NodeBuffer) : ReplicaDescriptorMessage;
	//decode(buffer: ByteArrayBuffer) : ReplicaDescriptorMessage;
	decode(buffer: ByteBuffer) : ReplicaDescriptorMessage;
	decode64(buffer: string) : ReplicaDescriptorMessage;
	
}
	
}


declare module cockroach.roachpb {
	
	export interface RangeDescriptor {
	
		

range_id?: Long;
		

getRangeId?() : Long;
		setRangeId?(rangeId : Long): void;
		



start_key?: ByteBuffer;
		

getStartKey?() : ByteBuffer;
		setStartKey?(startKey : ByteBuffer): void;
		



end_key?: ByteBuffer;
		

getEndKey?() : ByteBuffer;
		setEndKey?(endKey : ByteBuffer): void;
		



replicas?: ReplicaDescriptor[];
		

getReplicas?() : ReplicaDescriptor[];
		setReplicas?(replicas : ReplicaDescriptor[]): void;
		



next_replica_id?: number;
		

getNextReplicaId?() : number;
		setNextReplicaId?(nextReplicaId : number): void;
		



}
	
		
export interface RangeDescriptorMessage extends RangeDescriptor {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface RangeDescriptorBuilder {
	new(data?: RangeDescriptor): RangeDescriptorMessage;
	decode(buffer: ArrayBuffer) : RangeDescriptorMessage;
	//decode(buffer: NodeBuffer) : RangeDescriptorMessage;
	//decode(buffer: ByteArrayBuffer) : RangeDescriptorMessage;
	decode(buffer: ByteBuffer) : RangeDescriptorMessage;
	decode64(buffer: string) : RangeDescriptorMessage;
	
}
	
}


declare module cockroach.roachpb {
	
	export interface StoreCapacity {
	
		

capacity?: Long;
		

getCapacity?() : Long;
		setCapacity?(capacity : Long): void;
		



available?: Long;
		

getAvailable?() : Long;
		setAvailable?(available : Long): void;
		



range_count?: number;
		

getRangeCount?() : number;
		setRangeCount?(rangeCount : number): void;
		



}
	
		
export interface StoreCapacityMessage extends StoreCapacity {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface StoreCapacityBuilder {
	new(data?: StoreCapacity): StoreCapacityMessage;
	decode(buffer: ArrayBuffer) : StoreCapacityMessage;
	//decode(buffer: NodeBuffer) : StoreCapacityMessage;
	//decode(buffer: ByteArrayBuffer) : StoreCapacityMessage;
	decode(buffer: ByteBuffer) : StoreCapacityMessage;
	decode64(buffer: string) : StoreCapacityMessage;
	
}
	
}


declare module cockroach.roachpb {
	
	export interface NodeDescriptor {
	
		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



address?: util.UnresolvedAddr;
		

getAddress?() : util.UnresolvedAddr;
		setAddress?(address : util.UnresolvedAddr): void;
		



attrs?: Attributes;
		

getAttrs?() : Attributes;
		setAttrs?(attrs : Attributes): void;
		



}
	
		
export interface NodeDescriptorMessage extends NodeDescriptor {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface NodeDescriptorBuilder {
	new(data?: NodeDescriptor): NodeDescriptorMessage;
	decode(buffer: ArrayBuffer) : NodeDescriptorMessage;
	//decode(buffer: NodeBuffer) : NodeDescriptorMessage;
	//decode(buffer: ByteArrayBuffer) : NodeDescriptorMessage;
	decode(buffer: ByteBuffer) : NodeDescriptorMessage;
	decode64(buffer: string) : NodeDescriptorMessage;
	
}
	
}


declare module cockroach.roachpb {
	
	export interface StoreDescriptor {
	
		

store_id?: number;
		

getStoreId?() : number;
		setStoreId?(storeId : number): void;
		



attrs?: Attributes;
		

getAttrs?() : Attributes;
		setAttrs?(attrs : Attributes): void;
		



node?: NodeDescriptor;
		

getNode?() : NodeDescriptor;
		setNode?(node : NodeDescriptor): void;
		



capacity?: StoreCapacity;
		

getCapacity?() : StoreCapacity;
		setCapacity?(capacity : StoreCapacity): void;
		



}
	
		
export interface StoreDescriptorMessage extends StoreDescriptor {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface StoreDescriptorBuilder {
	new(data?: StoreDescriptor): StoreDescriptorMessage;
	decode(buffer: ArrayBuffer) : StoreDescriptorMessage;
	//decode(buffer: NodeBuffer) : StoreDescriptorMessage;
	//decode(buffer: ByteArrayBuffer) : StoreDescriptorMessage;
	decode(buffer: ByteBuffer) : StoreDescriptorMessage;
	decode64(buffer: string) : StoreDescriptorMessage;
	
}
	
}



declare module cockroach {
	
	export interface build {
	
		

}
	
		
export interface buildMessage extends build {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface buildBuilder {
	new(data?: build): buildMessage;
	decode(buffer: ArrayBuffer) : buildMessage;
	//decode(buffer: NodeBuffer) : buildMessage;
	//decode(buffer: ByteArrayBuffer) : buildMessage;
	decode(buffer: ByteBuffer) : buildMessage;
	decode64(buffer: string) : buildMessage;
	Info: build.InfoBuilder;
	
}
	
}

declare module cockroach.build {
	
	export interface Info {
	
		

go_version?: string;
		

getGoVersion?() : string;
		setGoVersion?(goVersion : string): void;
		



tag?: string;
		

getTag?() : string;
		setTag?(tag : string): void;
		



time?: string;
		

getTime?() : string;
		setTime?(time : string): void;
		



dependencies?: string;
		

getDependencies?() : string;
		setDependencies?(dependencies : string): void;
		



cgo_compiler?: string;
		

getCgoCompiler?() : string;
		setCgoCompiler?(cgoCompiler : string): void;
		



platform?: string;
		

getPlatform?() : string;
		setPlatform?(platform : string): void;
		



}
	
		
export interface InfoMessage extends Info {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface InfoBuilder {
	new(data?: Info): InfoMessage;
	decode(buffer: ArrayBuffer) : InfoMessage;
	//decode(buffer: NodeBuffer) : InfoMessage;
	//decode(buffer: ByteArrayBuffer) : InfoMessage;
	decode(buffer: ByteBuffer) : InfoMessage;
	decode64(buffer: string) : InfoMessage;
	
}
	
}



declare module cockroach {
	
	export interface server {
	
		

}
	
		
export interface serverMessage extends server {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface serverBuilder {
	new(data?: server): serverMessage;
	decode(buffer: ArrayBuffer) : serverMessage;
	//decode(buffer: NodeBuffer) : serverMessage;
	//decode(buffer: ByteArrayBuffer) : serverMessage;
	decode(buffer: ByteBuffer) : serverMessage;
	decode64(buffer: string) : serverMessage;
	status: server.statusBuilder;
	
}
	
}

declare module cockroach.server {
	
	export interface status {
	
		

}
	
		
export interface statusMessage extends status {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface statusBuilder {
	new(data?: status): statusMessage;
	decode(buffer: ArrayBuffer) : statusMessage;
	//decode(buffer: NodeBuffer) : statusMessage;
	//decode(buffer: ByteArrayBuffer) : statusMessage;
	decode(buffer: ByteBuffer) : statusMessage;
	decode64(buffer: string) : statusMessage;
	StoreStatus: status.StoreStatusBuilder;
	NodeStatus: status.NodeStatusBuilder;
	
}
	
}

declare module cockroach.server.status {
	
	export interface StoreStatus {
	
		

desc?: roachpb.StoreDescriptor;
		

getDesc?() : roachpb.StoreDescriptor;
		setDesc?(desc : roachpb.StoreDescriptor): void;
		



metrics?: ProtoBufMap<string, number>;
		

getMetrics?() : ProtoBufMap<string, number>;
		setMetrics?(metrics : ProtoBufMap<string, number>): void;
		



}
	
		
export interface StoreStatusMessage extends StoreStatus {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface StoreStatusBuilder {
	new(data?: StoreStatus): StoreStatusMessage;
	decode(buffer: ArrayBuffer) : StoreStatusMessage;
	//decode(buffer: NodeBuffer) : StoreStatusMessage;
	//decode(buffer: ByteArrayBuffer) : StoreStatusMessage;
	decode(buffer: ByteBuffer) : StoreStatusMessage;
	decode64(buffer: string) : StoreStatusMessage;
	
}
	
}


declare module cockroach.server.status {
	
	export interface NodeStatus {
	
		

desc?: roachpb.NodeDescriptor;
		

getDesc?() : roachpb.NodeDescriptor;
		setDesc?(desc : roachpb.NodeDescriptor): void;
		



build_info?: build.Info;
		

getBuildInfo?() : build.Info;
		setBuildInfo?(buildInfo : build.Info): void;
		



started_at?: Long;
		

getStartedAt?() : Long;
		setStartedAt?(startedAt : Long): void;
		



updated_at?: Long;
		

getUpdatedAt?() : Long;
		setUpdatedAt?(updatedAt : Long): void;
		



metrics?: ProtoBufMap<string, number>;
		

getMetrics?() : ProtoBufMap<string, number>;
		setMetrics?(metrics : ProtoBufMap<string, number>): void;
		



store_statuses?: StoreStatus[];
		

getStoreStatuses?() : StoreStatus[];
		setStoreStatuses?(storeStatuses : StoreStatus[]): void;
		



}
	
		
export interface NodeStatusMessage extends NodeStatus {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface NodeStatusBuilder {
	new(data?: NodeStatus): NodeStatusMessage;
	decode(buffer: ArrayBuffer) : NodeStatusMessage;
	//decode(buffer: NodeBuffer) : NodeStatusMessage;
	//decode(buffer: ByteArrayBuffer) : NodeStatusMessage;
	decode(buffer: ByteBuffer) : NodeStatusMessage;
	decode64(buffer: string) : NodeStatusMessage;
	
}
	
}




declare module cockroach {
	
	export interface ts {
	
		

}
	
		
export interface tsMessage extends ts {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface tsBuilder {
	new(data?: ts): tsMessage;
	decode(buffer: ArrayBuffer) : tsMessage;
	//decode(buffer: NodeBuffer) : tsMessage;
	//decode(buffer: ByteArrayBuffer) : tsMessage;
	decode(buffer: ByteBuffer) : tsMessage;
	decode64(buffer: string) : tsMessage;
	TimeSeriesDatapoint: ts.TimeSeriesDatapointBuilder;
	TimeSeriesData: ts.TimeSeriesDataBuilder;
	Query: ts.QueryBuilder;
	TimeSeriesQueryRequest: ts.TimeSeriesQueryRequestBuilder;
	TimeSeriesQueryResponse: ts.TimeSeriesQueryResponseBuilder;
	TimeSeriesQueryAggregator: ts.TimeSeriesQueryAggregator;
	TimeSeriesQueryDerivative: ts.TimeSeriesQueryDerivative;
	
}
	
}

declare module cockroach.ts {
	
	export interface TimeSeriesDatapoint {
	
		

timestamp_nanos?: Long;
		

getTimestampNanos?() : Long;
		setTimestampNanos?(timestampNanos : Long): void;
		



value?: number;
		

getValue?() : number;
		setValue?(value : number): void;
		



}
	
		
export interface TimeSeriesDatapointMessage extends TimeSeriesDatapoint {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesDatapointBuilder {
	new(data?: TimeSeriesDatapoint): TimeSeriesDatapointMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesDatapointMessage;
	//decode(buffer: NodeBuffer) : TimeSeriesDatapointMessage;
	//decode(buffer: ByteArrayBuffer) : TimeSeriesDatapointMessage;
	decode(buffer: ByteBuffer) : TimeSeriesDatapointMessage;
	decode64(buffer: string) : TimeSeriesDatapointMessage;
	
}
	
}


declare module cockroach.ts {
	
	export interface TimeSeriesData {
	
		

name?: string;
		

getName?() : string;
		setName?(name : string): void;
		



source?: string;
		

getSource?() : string;
		setSource?(source : string): void;
		



datapoints?: TimeSeriesDatapoint[];
		

getDatapoints?() : TimeSeriesDatapoint[];
		setDatapoints?(datapoints : TimeSeriesDatapoint[]): void;
		



}
	
		
export interface TimeSeriesDataMessage extends TimeSeriesData {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesDataBuilder {
	new(data?: TimeSeriesData): TimeSeriesDataMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesDataMessage;
	//decode(buffer: NodeBuffer) : TimeSeriesDataMessage;
	//decode(buffer: ByteArrayBuffer) : TimeSeriesDataMessage;
	decode(buffer: ByteBuffer) : TimeSeriesDataMessage;
	decode64(buffer: string) : TimeSeriesDataMessage;
	
}
	
}


declare module cockroach.ts {
	
	export interface Query {
	
		

name?: string;
		

getName?() : string;
		setName?(name : string): void;
		



downsampler?: TimeSeriesQueryAggregator;
		

getDownsampler?() : TimeSeriesQueryAggregator;
		setDownsampler?(downsampler : TimeSeriesQueryAggregator): void;
		



source_aggregator?: TimeSeriesQueryAggregator;
		

getSourceAggregator?() : TimeSeriesQueryAggregator;
		setSourceAggregator?(sourceAggregator : TimeSeriesQueryAggregator): void;
		



derivative?: TimeSeriesQueryDerivative;
		

getDerivative?() : TimeSeriesQueryDerivative;
		setDerivative?(derivative : TimeSeriesQueryDerivative): void;
		



sources?: string[];
		

getSources?() : string[];
		setSources?(sources : string[]): void;
		



}
	
		
export interface QueryMessage extends Query {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface QueryBuilder {
	new(data?: Query): QueryMessage;
	decode(buffer: ArrayBuffer) : QueryMessage;
	//decode(buffer: NodeBuffer) : QueryMessage;
	//decode(buffer: ByteArrayBuffer) : QueryMessage;
	decode(buffer: ByteBuffer) : QueryMessage;
	decode64(buffer: string) : QueryMessage;
	
}
	
}


declare module cockroach.ts {
	
	export interface TimeSeriesQueryRequest {
	
		

start_nanos?: Long;
		

getStartNanos?() : Long;
		setStartNanos?(startNanos : Long): void;
		



end_nanos?: Long;
		

getEndNanos?() : Long;
		setEndNanos?(endNanos : Long): void;
		



queries?: Query[];
		

getQueries?() : Query[];
		setQueries?(queries : Query[]): void;
		



}
	
		
export interface TimeSeriesQueryRequestMessage extends TimeSeriesQueryRequest {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesQueryRequestBuilder {
	new(data?: TimeSeriesQueryRequest): TimeSeriesQueryRequestMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesQueryRequestMessage;
	//decode(buffer: NodeBuffer) : TimeSeriesQueryRequestMessage;
	//decode(buffer: ByteArrayBuffer) : TimeSeriesQueryRequestMessage;
	decode(buffer: ByteBuffer) : TimeSeriesQueryRequestMessage;
	decode64(buffer: string) : TimeSeriesQueryRequestMessage;
	
}
	
}


declare module cockroach.ts {
	
	export interface TimeSeriesQueryResponse {
	
		

results?: TimeSeriesQueryResponse.Result[];
		

getResults?() : TimeSeriesQueryResponse.Result[];
		setResults?(results : TimeSeriesQueryResponse.Result[]): void;
		



}
	
		
export interface TimeSeriesQueryResponseMessage extends TimeSeriesQueryResponse {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesQueryResponseBuilder {
	new(data?: TimeSeriesQueryResponse): TimeSeriesQueryResponseMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesQueryResponseMessage;
	//decode(buffer: NodeBuffer) : TimeSeriesQueryResponseMessage;
	//decode(buffer: ByteArrayBuffer) : TimeSeriesQueryResponseMessage;
	decode(buffer: ByteBuffer) : TimeSeriesQueryResponseMessage;
	decode64(buffer: string) : TimeSeriesQueryResponseMessage;
	Result: TimeSeriesQueryResponse.ResultBuilder;
	
}
	
}

declare module cockroach.ts.TimeSeriesQueryResponse {
	
	export interface Result {
	
		

query?: Query;
		

getQuery?() : Query;
		setQuery?(query : Query): void;
		



datapoints?: TimeSeriesDatapoint[];
		

getDatapoints?() : TimeSeriesDatapoint[];
		setDatapoints?(datapoints : TimeSeriesDatapoint[]): void;
		



}
	
		
export interface ResultMessage extends Result {
	toArrayBuffer(): ArrayBuffer;
	//toBuffer(): NodeBuffer;
	encode(): ByteBuffer;
	toBase64(): string;
	toString(): string;
}

export interface ResultBuilder {
	new(data?: Result): ResultMessage;
	decode(buffer: ArrayBuffer) : ResultMessage;
	//decode(buffer: NodeBuffer) : ResultMessage;
	//decode(buffer: ByteArrayBuffer) : ResultMessage;
	decode(buffer: ByteBuffer) : ResultMessage;
	decode64(buffer: string) : ResultMessage;
	
}
	
}



declare module cockroach.ts {
	export const enum TimeSeriesQueryAggregator {
		AVG = 1,
		SUM = 2,
		MAX = 3,
		MIN = 4,
		
}
}

declare module cockroach.ts {
	export const enum TimeSeriesQueryDerivative {
		NONE = 0,
		DERIVATIVE = 1,
		NON_NEGATIVE_DERIVATIVE = 2,
		
}
}



