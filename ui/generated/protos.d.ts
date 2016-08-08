// GENERATED FILE DO NOT EDIT
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
		storage: storageBuilder;
		config: configBuilder;
		server: serverBuilder;
		build: buildBuilder;
		gossip: gossipBuilder;
		ts: tsBuilder;
		
}
}

declare module cockroach {

	export interface util {

		

}

	export interface utilMessage extends util {
	add(key: string, value: any, noAssert?: boolean): utilMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): utilMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface utilBuilder {
	new(data?: util): utilMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : utilMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : utilMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : utilMessage;
	decode(buffer: string, length?: number | string, enc?: string) : utilMessage;
	decode64(str: string) : utilMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): utilMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): utilMessage;
	decodeDelimited(buffer: Buffer, enc: string): utilMessage;
	decodeDelimited(buffer: string, enc: string): utilMessage;
	decodeHex(str: string): utilMessage;
	decodeJSON(str: string): utilMessage;
	UnresolvedAddr: util.UnresolvedAddrBuilder;
	hlc: util.hlcBuilder;
	
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
	add(key: string, value: any, noAssert?: boolean): UnresolvedAddrMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): UnresolvedAddrMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface UnresolvedAddrBuilder {
	new(data?: UnresolvedAddr): UnresolvedAddrMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : UnresolvedAddrMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : UnresolvedAddrMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : UnresolvedAddrMessage;
	decode(buffer: string, length?: number | string, enc?: string) : UnresolvedAddrMessage;
	decode64(str: string) : UnresolvedAddrMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): UnresolvedAddrMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): UnresolvedAddrMessage;
	decodeDelimited(buffer: Buffer, enc: string): UnresolvedAddrMessage;
	decodeDelimited(buffer: string, enc: string): UnresolvedAddrMessage;
	decodeHex(str: string): UnresolvedAddrMessage;
	decodeJSON(str: string): UnresolvedAddrMessage;
	
}

}


declare module cockroach.util {

	export interface hlc {

		

}

	export interface hlcMessage extends hlc {
	add(key: string, value: any, noAssert?: boolean): hlcMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): hlcMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface hlcBuilder {
	new(data?: hlc): hlcMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : hlcMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : hlcMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : hlcMessage;
	decode(buffer: string, length?: number | string, enc?: string) : hlcMessage;
	decode64(str: string) : hlcMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): hlcMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): hlcMessage;
	decodeDelimited(buffer: Buffer, enc: string): hlcMessage;
	decodeDelimited(buffer: string, enc: string): hlcMessage;
	decodeHex(str: string): hlcMessage;
	decodeJSON(str: string): hlcMessage;
	Timestamp: hlc.TimestampBuilder;
	
}

}

declare module cockroach.util.hlc {

	export interface Timestamp {

		

wall_time?: Long;
		

getWallTime?() : Long;
		setWallTime?(wallTime : Long): void;
		



logical?: number;
		

getLogical?() : number;
		setLogical?(logical : number): void;
		



}

	export interface TimestampMessage extends Timestamp {
	add(key: string, value: any, noAssert?: boolean): TimestampMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimestampMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimestampMessage;
	decode64(str: string) : TimestampMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: string, enc: string): TimestampMessage;
	decodeHex(str: string): TimestampMessage;
	decodeJSON(str: string): TimestampMessage;
	
}

}




declare module cockroach {

	export interface roachpb {

		

}

	export interface roachpbMessage extends roachpb {
	add(key: string, value: any, noAssert?: boolean): roachpbMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): roachpbMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface roachpbBuilder {
	new(data?: roachpb): roachpbMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : roachpbMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : roachpbMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : roachpbMessage;
	decode(buffer: string, length?: number | string, enc?: string) : roachpbMessage;
	decode64(str: string) : roachpbMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): roachpbMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): roachpbMessage;
	decodeDelimited(buffer: Buffer, enc: string): roachpbMessage;
	decodeDelimited(buffer: string, enc: string): roachpbMessage;
	decodeHex(str: string): roachpbMessage;
	decodeJSON(str: string): roachpbMessage;
	Attributes: roachpb.AttributesBuilder;
	ReplicaDescriptor: roachpb.ReplicaDescriptorBuilder;
	ReplicaIdent: roachpb.ReplicaIdentBuilder;
	RangeDescriptor: roachpb.RangeDescriptorBuilder;
	StoreCapacity: roachpb.StoreCapacityBuilder;
	NodeDescriptor: roachpb.NodeDescriptorBuilder;
	StoreDescriptor: roachpb.StoreDescriptorBuilder;
	StoreDeadReplicas: roachpb.StoreDeadReplicasBuilder;
	Span: roachpb.SpanBuilder;
	Value: roachpb.ValueBuilder;
	KeyValue: roachpb.KeyValueBuilder;
	StoreIdent: roachpb.StoreIdentBuilder;
	SplitTrigger: roachpb.SplitTriggerBuilder;
	MergeTrigger: roachpb.MergeTriggerBuilder;
	ChangeReplicasTrigger: roachpb.ChangeReplicasTriggerBuilder;
	ModifiedSpanTrigger: roachpb.ModifiedSpanTriggerBuilder;
	InternalCommitTrigger: roachpb.InternalCommitTriggerBuilder;
	Transaction: roachpb.TransactionBuilder;
	Intent: roachpb.IntentBuilder;
	Lease: roachpb.LeaseBuilder;
	AbortCacheEntry: roachpb.AbortCacheEntryBuilder;
	RaftTruncatedState: roachpb.RaftTruncatedStateBuilder;
	RaftTombstone: roachpb.RaftTombstoneBuilder;
	RaftSnapshotData: roachpb.RaftSnapshotDataBuilder;
	ValueType: roachpb.ValueType;
	ReplicaChangeType: roachpb.ReplicaChangeType;
	TransactionStatus: roachpb.TransactionStatus;
	
}

}

declare module cockroach.roachpb {

	export interface Attributes {

		

attrs?: string[];
		

getAttrs?() : string[];
		setAttrs?(attrs : string[]): void;
		



}

	export interface AttributesMessage extends Attributes {
	add(key: string, value: any, noAssert?: boolean): AttributesMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): AttributesMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface AttributesBuilder {
	new(data?: Attributes): AttributesMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : AttributesMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : AttributesMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : AttributesMessage;
	decode(buffer: string, length?: number | string, enc?: string) : AttributesMessage;
	decode64(str: string) : AttributesMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): AttributesMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): AttributesMessage;
	decodeDelimited(buffer: Buffer, enc: string): AttributesMessage;
	decodeDelimited(buffer: string, enc: string): AttributesMessage;
	decodeHex(str: string): AttributesMessage;
	decodeJSON(str: string): AttributesMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): ReplicaDescriptorMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ReplicaDescriptorMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ReplicaDescriptorBuilder {
	new(data?: ReplicaDescriptor): ReplicaDescriptorMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ReplicaDescriptorMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ReplicaDescriptorMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ReplicaDescriptorMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ReplicaDescriptorMessage;
	decode64(str: string) : ReplicaDescriptorMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ReplicaDescriptorMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ReplicaDescriptorMessage;
	decodeDelimited(buffer: Buffer, enc: string): ReplicaDescriptorMessage;
	decodeDelimited(buffer: string, enc: string): ReplicaDescriptorMessage;
	decodeHex(str: string): ReplicaDescriptorMessage;
	decodeJSON(str: string): ReplicaDescriptorMessage;
	
}

}


declare module cockroach.roachpb {

	export interface ReplicaIdent {

		

range_id?: Long;
		

getRangeId?() : Long;
		setRangeId?(rangeId : Long): void;
		



replica?: ReplicaDescriptor;
		

getReplica?() : ReplicaDescriptor;
		setReplica?(replica : ReplicaDescriptor): void;
		



}

	export interface ReplicaIdentMessage extends ReplicaIdent {
	add(key: string, value: any, noAssert?: boolean): ReplicaIdentMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ReplicaIdentMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ReplicaIdentBuilder {
	new(data?: ReplicaIdent): ReplicaIdentMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ReplicaIdentMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ReplicaIdentMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ReplicaIdentMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ReplicaIdentMessage;
	decode64(str: string) : ReplicaIdentMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ReplicaIdentMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ReplicaIdentMessage;
	decodeDelimited(buffer: Buffer, enc: string): ReplicaIdentMessage;
	decodeDelimited(buffer: string, enc: string): ReplicaIdentMessage;
	decodeHex(str: string): ReplicaIdentMessage;
	decodeJSON(str: string): ReplicaIdentMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): RangeDescriptorMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RangeDescriptorMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RangeDescriptorBuilder {
	new(data?: RangeDescriptor): RangeDescriptorMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RangeDescriptorMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RangeDescriptorMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RangeDescriptorMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RangeDescriptorMessage;
	decode64(str: string) : RangeDescriptorMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RangeDescriptorMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RangeDescriptorMessage;
	decodeDelimited(buffer: Buffer, enc: string): RangeDescriptorMessage;
	decodeDelimited(buffer: string, enc: string): RangeDescriptorMessage;
	decodeHex(str: string): RangeDescriptorMessage;
	decodeJSON(str: string): RangeDescriptorMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): StoreCapacityMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StoreCapacityMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StoreCapacityBuilder {
	new(data?: StoreCapacity): StoreCapacityMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StoreCapacityMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StoreCapacityMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StoreCapacityMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StoreCapacityMessage;
	decode64(str: string) : StoreCapacityMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StoreCapacityMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StoreCapacityMessage;
	decodeDelimited(buffer: Buffer, enc: string): StoreCapacityMessage;
	decodeDelimited(buffer: string, enc: string): StoreCapacityMessage;
	decodeHex(str: string): StoreCapacityMessage;
	decodeJSON(str: string): StoreCapacityMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): NodeDescriptorMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): NodeDescriptorMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface NodeDescriptorBuilder {
	new(data?: NodeDescriptor): NodeDescriptorMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : NodeDescriptorMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : NodeDescriptorMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : NodeDescriptorMessage;
	decode(buffer: string, length?: number | string, enc?: string) : NodeDescriptorMessage;
	decode64(str: string) : NodeDescriptorMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): NodeDescriptorMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): NodeDescriptorMessage;
	decodeDelimited(buffer: Buffer, enc: string): NodeDescriptorMessage;
	decodeDelimited(buffer: string, enc: string): NodeDescriptorMessage;
	decodeHex(str: string): NodeDescriptorMessage;
	decodeJSON(str: string): NodeDescriptorMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): StoreDescriptorMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StoreDescriptorMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StoreDescriptorBuilder {
	new(data?: StoreDescriptor): StoreDescriptorMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StoreDescriptorMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StoreDescriptorMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StoreDescriptorMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StoreDescriptorMessage;
	decode64(str: string) : StoreDescriptorMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StoreDescriptorMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StoreDescriptorMessage;
	decodeDelimited(buffer: Buffer, enc: string): StoreDescriptorMessage;
	decodeDelimited(buffer: string, enc: string): StoreDescriptorMessage;
	decodeHex(str: string): StoreDescriptorMessage;
	decodeJSON(str: string): StoreDescriptorMessage;
	
}

}


declare module cockroach.roachpb {

	export interface StoreDeadReplicas {

		

store_id?: number;
		

getStoreId?() : number;
		setStoreId?(storeId : number): void;
		



replicas?: ReplicaIdent[];
		

getReplicas?() : ReplicaIdent[];
		setReplicas?(replicas : ReplicaIdent[]): void;
		



}

	export interface StoreDeadReplicasMessage extends StoreDeadReplicas {
	add(key: string, value: any, noAssert?: boolean): StoreDeadReplicasMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StoreDeadReplicasMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StoreDeadReplicasBuilder {
	new(data?: StoreDeadReplicas): StoreDeadReplicasMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StoreDeadReplicasMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StoreDeadReplicasMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StoreDeadReplicasMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StoreDeadReplicasMessage;
	decode64(str: string) : StoreDeadReplicasMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StoreDeadReplicasMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StoreDeadReplicasMessage;
	decodeDelimited(buffer: Buffer, enc: string): StoreDeadReplicasMessage;
	decodeDelimited(buffer: string, enc: string): StoreDeadReplicasMessage;
	decodeHex(str: string): StoreDeadReplicasMessage;
	decodeJSON(str: string): StoreDeadReplicasMessage;
	
}

}


declare module cockroach.roachpb {

	export interface Span {

		

key?: ByteBuffer;
		

getKey?() : ByteBuffer;
		setKey?(key : ByteBuffer): void;
		



end_key?: ByteBuffer;
		

getEndKey?() : ByteBuffer;
		setEndKey?(endKey : ByteBuffer): void;
		



}

	export interface SpanMessage extends Span {
	add(key: string, value: any, noAssert?: boolean): SpanMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SpanMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SpanBuilder {
	new(data?: Span): SpanMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SpanMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SpanMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SpanMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SpanMessage;
	decode64(str: string) : SpanMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SpanMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SpanMessage;
	decodeDelimited(buffer: Buffer, enc: string): SpanMessage;
	decodeDelimited(buffer: string, enc: string): SpanMessage;
	decodeHex(str: string): SpanMessage;
	decodeJSON(str: string): SpanMessage;
	
}

}


declare module cockroach.roachpb {

	export interface Value {

		

raw_bytes?: ByteBuffer;
		

getRawBytes?() : ByteBuffer;
		setRawBytes?(rawBytes : ByteBuffer): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



}

	export interface ValueMessage extends Value {
	add(key: string, value: any, noAssert?: boolean): ValueMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ValueMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ValueBuilder {
	new(data?: Value): ValueMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ValueMessage;
	decode64(str: string) : ValueMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ValueMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ValueMessage;
	decodeDelimited(buffer: Buffer, enc: string): ValueMessage;
	decodeDelimited(buffer: string, enc: string): ValueMessage;
	decodeHex(str: string): ValueMessage;
	decodeJSON(str: string): ValueMessage;
	
}

}


declare module cockroach.roachpb {

	export interface KeyValue {

		

key?: ByteBuffer;
		

getKey?() : ByteBuffer;
		setKey?(key : ByteBuffer): void;
		



value?: Value;
		

getValue?() : Value;
		setValue?(value : Value): void;
		



}

	export interface KeyValueMessage extends KeyValue {
	add(key: string, value: any, noAssert?: boolean): KeyValueMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): KeyValueMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface KeyValueBuilder {
	new(data?: KeyValue): KeyValueMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: string, length?: number | string, enc?: string) : KeyValueMessage;
	decode64(str: string) : KeyValueMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: Buffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: string, enc: string): KeyValueMessage;
	decodeHex(str: string): KeyValueMessage;
	decodeJSON(str: string): KeyValueMessage;
	
}

}


declare module cockroach.roachpb {

	export interface StoreIdent {

		

cluster_id?: ByteBuffer;
		

getClusterId?() : ByteBuffer;
		setClusterId?(clusterId : ByteBuffer): void;
		



node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



store_id?: number;
		

getStoreId?() : number;
		setStoreId?(storeId : number): void;
		



}

	export interface StoreIdentMessage extends StoreIdent {
	add(key: string, value: any, noAssert?: boolean): StoreIdentMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StoreIdentMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StoreIdentBuilder {
	new(data?: StoreIdent): StoreIdentMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StoreIdentMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StoreIdentMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StoreIdentMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StoreIdentMessage;
	decode64(str: string) : StoreIdentMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StoreIdentMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StoreIdentMessage;
	decodeDelimited(buffer: Buffer, enc: string): StoreIdentMessage;
	decodeDelimited(buffer: string, enc: string): StoreIdentMessage;
	decodeHex(str: string): StoreIdentMessage;
	decodeJSON(str: string): StoreIdentMessage;
	
}

}


declare module cockroach.roachpb {

	export interface SplitTrigger {

		

left_desc?: RangeDescriptor;
		

getLeftDesc?() : RangeDescriptor;
		setLeftDesc?(leftDesc : RangeDescriptor): void;
		



right_desc?: RangeDescriptor;
		

getRightDesc?() : RangeDescriptor;
		setRightDesc?(rightDesc : RangeDescriptor): void;
		



}

	export interface SplitTriggerMessage extends SplitTrigger {
	add(key: string, value: any, noAssert?: boolean): SplitTriggerMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SplitTriggerMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SplitTriggerBuilder {
	new(data?: SplitTrigger): SplitTriggerMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SplitTriggerMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SplitTriggerMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SplitTriggerMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SplitTriggerMessage;
	decode64(str: string) : SplitTriggerMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SplitTriggerMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SplitTriggerMessage;
	decodeDelimited(buffer: Buffer, enc: string): SplitTriggerMessage;
	decodeDelimited(buffer: string, enc: string): SplitTriggerMessage;
	decodeHex(str: string): SplitTriggerMessage;
	decodeJSON(str: string): SplitTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface MergeTrigger {

		

left_desc?: RangeDescriptor;
		

getLeftDesc?() : RangeDescriptor;
		setLeftDesc?(leftDesc : RangeDescriptor): void;
		



right_desc?: RangeDescriptor;
		

getRightDesc?() : RangeDescriptor;
		setRightDesc?(rightDesc : RangeDescriptor): void;
		



}

	export interface MergeTriggerMessage extends MergeTrigger {
	add(key: string, value: any, noAssert?: boolean): MergeTriggerMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): MergeTriggerMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface MergeTriggerBuilder {
	new(data?: MergeTrigger): MergeTriggerMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : MergeTriggerMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : MergeTriggerMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : MergeTriggerMessage;
	decode(buffer: string, length?: number | string, enc?: string) : MergeTriggerMessage;
	decode64(str: string) : MergeTriggerMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): MergeTriggerMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): MergeTriggerMessage;
	decodeDelimited(buffer: Buffer, enc: string): MergeTriggerMessage;
	decodeDelimited(buffer: string, enc: string): MergeTriggerMessage;
	decodeHex(str: string): MergeTriggerMessage;
	decodeJSON(str: string): MergeTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface ChangeReplicasTrigger {

		

change_type?: ReplicaChangeType;
		

getChangeType?() : ReplicaChangeType;
		setChangeType?(changeType : ReplicaChangeType): void;
		



replica?: ReplicaDescriptor;
		

getReplica?() : ReplicaDescriptor;
		setReplica?(replica : ReplicaDescriptor): void;
		



updated_replicas?: ReplicaDescriptor[];
		

getUpdatedReplicas?() : ReplicaDescriptor[];
		setUpdatedReplicas?(updatedReplicas : ReplicaDescriptor[]): void;
		



next_replica_id?: number;
		

getNextReplicaId?() : number;
		setNextReplicaId?(nextReplicaId : number): void;
		



}

	export interface ChangeReplicasTriggerMessage extends ChangeReplicasTrigger {
	add(key: string, value: any, noAssert?: boolean): ChangeReplicasTriggerMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ChangeReplicasTriggerMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ChangeReplicasTriggerBuilder {
	new(data?: ChangeReplicasTrigger): ChangeReplicasTriggerMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ChangeReplicasTriggerMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ChangeReplicasTriggerMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ChangeReplicasTriggerMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ChangeReplicasTriggerMessage;
	decode64(str: string) : ChangeReplicasTriggerMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ChangeReplicasTriggerMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ChangeReplicasTriggerMessage;
	decodeDelimited(buffer: Buffer, enc: string): ChangeReplicasTriggerMessage;
	decodeDelimited(buffer: string, enc: string): ChangeReplicasTriggerMessage;
	decodeHex(str: string): ChangeReplicasTriggerMessage;
	decodeJSON(str: string): ChangeReplicasTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface ModifiedSpanTrigger {

		

system_config_span?: boolean;
		

getSystemConfigSpan?() : boolean;
		setSystemConfigSpan?(systemConfigSpan : boolean): void;
		



}

	export interface ModifiedSpanTriggerMessage extends ModifiedSpanTrigger {
	add(key: string, value: any, noAssert?: boolean): ModifiedSpanTriggerMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ModifiedSpanTriggerMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ModifiedSpanTriggerBuilder {
	new(data?: ModifiedSpanTrigger): ModifiedSpanTriggerMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ModifiedSpanTriggerMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ModifiedSpanTriggerMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ModifiedSpanTriggerMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ModifiedSpanTriggerMessage;
	decode64(str: string) : ModifiedSpanTriggerMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ModifiedSpanTriggerMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ModifiedSpanTriggerMessage;
	decodeDelimited(buffer: Buffer, enc: string): ModifiedSpanTriggerMessage;
	decodeDelimited(buffer: string, enc: string): ModifiedSpanTriggerMessage;
	decodeHex(str: string): ModifiedSpanTriggerMessage;
	decodeJSON(str: string): ModifiedSpanTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface InternalCommitTrigger {

		

split_trigger?: SplitTrigger;
		

getSplitTrigger?() : SplitTrigger;
		setSplitTrigger?(splitTrigger : SplitTrigger): void;
		



merge_trigger?: MergeTrigger;
		

getMergeTrigger?() : MergeTrigger;
		setMergeTrigger?(mergeTrigger : MergeTrigger): void;
		



change_replicas_trigger?: ChangeReplicasTrigger;
		

getChangeReplicasTrigger?() : ChangeReplicasTrigger;
		setChangeReplicasTrigger?(changeReplicasTrigger : ChangeReplicasTrigger): void;
		



modified_span_trigger?: ModifiedSpanTrigger;
		

getModifiedSpanTrigger?() : ModifiedSpanTrigger;
		setModifiedSpanTrigger?(modifiedSpanTrigger : ModifiedSpanTrigger): void;
		



}

	export interface InternalCommitTriggerMessage extends InternalCommitTrigger {
	add(key: string, value: any, noAssert?: boolean): InternalCommitTriggerMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): InternalCommitTriggerMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface InternalCommitTriggerBuilder {
	new(data?: InternalCommitTrigger): InternalCommitTriggerMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : InternalCommitTriggerMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : InternalCommitTriggerMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : InternalCommitTriggerMessage;
	decode(buffer: string, length?: number | string, enc?: string) : InternalCommitTriggerMessage;
	decode64(str: string) : InternalCommitTriggerMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): InternalCommitTriggerMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): InternalCommitTriggerMessage;
	decodeDelimited(buffer: Buffer, enc: string): InternalCommitTriggerMessage;
	decodeDelimited(buffer: string, enc: string): InternalCommitTriggerMessage;
	decodeHex(str: string): InternalCommitTriggerMessage;
	decodeJSON(str: string): InternalCommitTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface Transaction {

		

meta?: storage.engine.enginepb.TxnMeta;
		

getMeta?() : storage.engine.enginepb.TxnMeta;
		setMeta?(meta : storage.engine.enginepb.TxnMeta): void;
		



name?: string;
		

getName?() : string;
		setName?(name : string): void;
		



status?: TransactionStatus;
		

getStatus?() : TransactionStatus;
		setStatus?(status : TransactionStatus): void;
		



last_heartbeat?: util.hlc.Timestamp;
		

getLastHeartbeat?() : util.hlc.Timestamp;
		setLastHeartbeat?(lastHeartbeat : util.hlc.Timestamp): void;
		



orig_timestamp?: util.hlc.Timestamp;
		

getOrigTimestamp?() : util.hlc.Timestamp;
		setOrigTimestamp?(origTimestamp : util.hlc.Timestamp): void;
		



max_timestamp?: util.hlc.Timestamp;
		

getMaxTimestamp?() : util.hlc.Timestamp;
		setMaxTimestamp?(maxTimestamp : util.hlc.Timestamp): void;
		



observed_timestamps?: ProtoBufMap<number, util.hlc.Timestamp>;
		

getObservedTimestamps?() : ProtoBufMap<number, util.hlc.Timestamp>;
		setObservedTimestamps?(observedTimestamps : ProtoBufMap<number, util.hlc.Timestamp>): void;
		



writing?: boolean;
		

getWriting?() : boolean;
		setWriting?(writing : boolean): void;
		



write_too_old?: boolean;
		

getWriteTooOld?() : boolean;
		setWriteTooOld?(writeTooOld : boolean): void;
		



retry_on_push?: boolean;
		

getRetryOnPush?() : boolean;
		setRetryOnPush?(retryOnPush : boolean): void;
		



intents?: Span[];
		

getIntents?() : Span[];
		setIntents?(intents : Span[]): void;
		



}

	export interface TransactionMessage extends Transaction {
	add(key: string, value: any, noAssert?: boolean): TransactionMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TransactionMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TransactionBuilder {
	new(data?: Transaction): TransactionMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TransactionMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TransactionMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TransactionMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TransactionMessage;
	decode64(str: string) : TransactionMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TransactionMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TransactionMessage;
	decodeDelimited(buffer: Buffer, enc: string): TransactionMessage;
	decodeDelimited(buffer: string, enc: string): TransactionMessage;
	decodeHex(str: string): TransactionMessage;
	decodeJSON(str: string): TransactionMessage;
	
}

}


declare module cockroach.roachpb {

	export interface Intent {

		

span?: Span;
		

getSpan?() : Span;
		setSpan?(span : Span): void;
		



txn?: storage.engine.enginepb.TxnMeta;
		

getTxn?() : storage.engine.enginepb.TxnMeta;
		setTxn?(txn : storage.engine.enginepb.TxnMeta): void;
		



status?: TransactionStatus;
		

getStatus?() : TransactionStatus;
		setStatus?(status : TransactionStatus): void;
		



}

	export interface IntentMessage extends Intent {
	add(key: string, value: any, noAssert?: boolean): IntentMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): IntentMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface IntentBuilder {
	new(data?: Intent): IntentMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : IntentMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : IntentMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : IntentMessage;
	decode(buffer: string, length?: number | string, enc?: string) : IntentMessage;
	decode64(str: string) : IntentMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): IntentMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): IntentMessage;
	decodeDelimited(buffer: Buffer, enc: string): IntentMessage;
	decodeDelimited(buffer: string, enc: string): IntentMessage;
	decodeHex(str: string): IntentMessage;
	decodeJSON(str: string): IntentMessage;
	
}

}


declare module cockroach.roachpb {

	export interface Lease {

		

start?: util.hlc.Timestamp;
		

getStart?() : util.hlc.Timestamp;
		setStart?(start : util.hlc.Timestamp): void;
		



start_stasis?: util.hlc.Timestamp;
		

getStartStasis?() : util.hlc.Timestamp;
		setStartStasis?(startStasis : util.hlc.Timestamp): void;
		



expiration?: util.hlc.Timestamp;
		

getExpiration?() : util.hlc.Timestamp;
		setExpiration?(expiration : util.hlc.Timestamp): void;
		



replica?: ReplicaDescriptor;
		

getReplica?() : ReplicaDescriptor;
		setReplica?(replica : ReplicaDescriptor): void;
		



}

	export interface LeaseMessage extends Lease {
	add(key: string, value: any, noAssert?: boolean): LeaseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): LeaseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface LeaseBuilder {
	new(data?: Lease): LeaseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : LeaseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : LeaseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : LeaseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : LeaseMessage;
	decode64(str: string) : LeaseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): LeaseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): LeaseMessage;
	decodeDelimited(buffer: Buffer, enc: string): LeaseMessage;
	decodeDelimited(buffer: string, enc: string): LeaseMessage;
	decodeHex(str: string): LeaseMessage;
	decodeJSON(str: string): LeaseMessage;
	
}

}


declare module cockroach.roachpb {

	export interface AbortCacheEntry {

		

key?: ByteBuffer;
		

getKey?() : ByteBuffer;
		setKey?(key : ByteBuffer): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



priority?: number;
		

getPriority?() : number;
		setPriority?(priority : number): void;
		



}

	export interface AbortCacheEntryMessage extends AbortCacheEntry {
	add(key: string, value: any, noAssert?: boolean): AbortCacheEntryMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): AbortCacheEntryMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface AbortCacheEntryBuilder {
	new(data?: AbortCacheEntry): AbortCacheEntryMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : AbortCacheEntryMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : AbortCacheEntryMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : AbortCacheEntryMessage;
	decode(buffer: string, length?: number | string, enc?: string) : AbortCacheEntryMessage;
	decode64(str: string) : AbortCacheEntryMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): AbortCacheEntryMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): AbortCacheEntryMessage;
	decodeDelimited(buffer: Buffer, enc: string): AbortCacheEntryMessage;
	decodeDelimited(buffer: string, enc: string): AbortCacheEntryMessage;
	decodeHex(str: string): AbortCacheEntryMessage;
	decodeJSON(str: string): AbortCacheEntryMessage;
	
}

}


declare module cockroach.roachpb {

	export interface RaftTruncatedState {

		

index?: Long;
		

getIndex?() : Long;
		setIndex?(index : Long): void;
		



term?: Long;
		

getTerm?() : Long;
		setTerm?(term : Long): void;
		



}

	export interface RaftTruncatedStateMessage extends RaftTruncatedState {
	add(key: string, value: any, noAssert?: boolean): RaftTruncatedStateMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftTruncatedStateMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftTruncatedStateBuilder {
	new(data?: RaftTruncatedState): RaftTruncatedStateMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftTruncatedStateMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftTruncatedStateMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftTruncatedStateMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftTruncatedStateMessage;
	decode64(str: string) : RaftTruncatedStateMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftTruncatedStateMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftTruncatedStateMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftTruncatedStateMessage;
	decodeDelimited(buffer: string, enc: string): RaftTruncatedStateMessage;
	decodeHex(str: string): RaftTruncatedStateMessage;
	decodeJSON(str: string): RaftTruncatedStateMessage;
	
}

}


declare module cockroach.roachpb {

	export interface RaftTombstone {

		

next_replica_id?: number;
		

getNextReplicaId?() : number;
		setNextReplicaId?(nextReplicaId : number): void;
		



}

	export interface RaftTombstoneMessage extends RaftTombstone {
	add(key: string, value: any, noAssert?: boolean): RaftTombstoneMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftTombstoneMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftTombstoneBuilder {
	new(data?: RaftTombstone): RaftTombstoneMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftTombstoneMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftTombstoneMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftTombstoneMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftTombstoneMessage;
	decode64(str: string) : RaftTombstoneMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftTombstoneMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftTombstoneMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftTombstoneMessage;
	decodeDelimited(buffer: string, enc: string): RaftTombstoneMessage;
	decodeHex(str: string): RaftTombstoneMessage;
	decodeJSON(str: string): RaftTombstoneMessage;
	
}

}


declare module cockroach.roachpb {

	export interface RaftSnapshotData {

		

range_descriptor?: RangeDescriptor;
		

getRangeDescriptor?() : RangeDescriptor;
		setRangeDescriptor?(rangeDescriptor : RangeDescriptor): void;
		



KV?: RaftSnapshotData.KeyValue[];
		

getKV?() : RaftSnapshotData.KeyValue[];
		setKV?(kV : RaftSnapshotData.KeyValue[]): void;
		



log_entries?: ByteBuffer[];
		

getLogEntries?() : ByteBuffer[];
		setLogEntries?(logEntries : ByteBuffer[]): void;
		



}

	export interface RaftSnapshotDataMessage extends RaftSnapshotData {
	add(key: string, value: any, noAssert?: boolean): RaftSnapshotDataMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftSnapshotDataMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftSnapshotDataBuilder {
	new(data?: RaftSnapshotData): RaftSnapshotDataMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftSnapshotDataMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftSnapshotDataMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftSnapshotDataMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftSnapshotDataMessage;
	decode64(str: string) : RaftSnapshotDataMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftSnapshotDataMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftSnapshotDataMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftSnapshotDataMessage;
	decodeDelimited(buffer: string, enc: string): RaftSnapshotDataMessage;
	decodeHex(str: string): RaftSnapshotDataMessage;
	decodeJSON(str: string): RaftSnapshotDataMessage;
	KeyValue: RaftSnapshotData.KeyValueBuilder;
	
}

}

declare module cockroach.roachpb.RaftSnapshotData {

	export interface KeyValue {

		

key?: ByteBuffer;
		

getKey?() : ByteBuffer;
		setKey?(key : ByteBuffer): void;
		



value?: ByteBuffer;
		

getValue?() : ByteBuffer;
		setValue?(value : ByteBuffer): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



}

	export interface KeyValueMessage extends KeyValue {
	add(key: string, value: any, noAssert?: boolean): KeyValueMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): KeyValueMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface KeyValueBuilder {
	new(data?: KeyValue): KeyValueMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : KeyValueMessage;
	decode(buffer: string, length?: number | string, enc?: string) : KeyValueMessage;
	decode64(str: string) : KeyValueMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: Buffer, enc: string): KeyValueMessage;
	decodeDelimited(buffer: string, enc: string): KeyValueMessage;
	decodeHex(str: string): KeyValueMessage;
	decodeJSON(str: string): KeyValueMessage;
	
}

}



declare module cockroach.roachpb {
	export const enum ValueType {
		UNKNOWN = 0,
		NULL = 7,
		INT = 1,
		FLOAT = 2,
		BYTES = 3,
		DELIMITED_BYTES = 8,
		TIME = 4,
		DECIMAL = 5,
		DELIMITED_DECIMAL = 9,
		DURATION = 6,
		TUPLE = 10,
		TIMESERIES = 100,
		
}
}

declare module cockroach.roachpb {
	export const enum ReplicaChangeType {
		ADD_REPLICA = 0,
		REMOVE_REPLICA = 1,
		
}
}

declare module cockroach.roachpb {
	export const enum TransactionStatus {
		PENDING = 0,
		COMMITTED = 1,
		ABORTED = 2,
		
}
}


declare module cockroach {

	export interface storage {

		

}

	export interface storageMessage extends storage {
	add(key: string, value: any, noAssert?: boolean): storageMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): storageMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface storageBuilder {
	new(data?: storage): storageMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : storageMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : storageMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : storageMessage;
	decode(buffer: string, length?: number | string, enc?: string) : storageMessage;
	decode64(str: string) : storageMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): storageMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): storageMessage;
	decodeDelimited(buffer: Buffer, enc: string): storageMessage;
	decodeDelimited(buffer: string, enc: string): storageMessage;
	decodeHex(str: string): storageMessage;
	decodeJSON(str: string): storageMessage;
	engine: storage.engineBuilder;
	storagebase: storage.storagebaseBuilder;
	
}

}

declare module cockroach.storage {

	export interface engine {

		

}

	export interface engineMessage extends engine {
	add(key: string, value: any, noAssert?: boolean): engineMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): engineMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface engineBuilder {
	new(data?: engine): engineMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : engineMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : engineMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : engineMessage;
	decode(buffer: string, length?: number | string, enc?: string) : engineMessage;
	decode64(str: string) : engineMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): engineMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): engineMessage;
	decodeDelimited(buffer: Buffer, enc: string): engineMessage;
	decodeDelimited(buffer: string, enc: string): engineMessage;
	decodeHex(str: string): engineMessage;
	decodeJSON(str: string): engineMessage;
	enginepb: engine.enginepbBuilder;
	
}

}

declare module cockroach.storage.engine {

	export interface enginepb {

		

}

	export interface enginepbMessage extends enginepb {
	add(key: string, value: any, noAssert?: boolean): enginepbMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): enginepbMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface enginepbBuilder {
	new(data?: enginepb): enginepbMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : enginepbMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : enginepbMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : enginepbMessage;
	decode(buffer: string, length?: number | string, enc?: string) : enginepbMessage;
	decode64(str: string) : enginepbMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): enginepbMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): enginepbMessage;
	decodeDelimited(buffer: Buffer, enc: string): enginepbMessage;
	decodeDelimited(buffer: string, enc: string): enginepbMessage;
	decodeHex(str: string): enginepbMessage;
	decodeJSON(str: string): enginepbMessage;
	TxnMeta: enginepb.TxnMetaBuilder;
	MVCCMetadata: enginepb.MVCCMetadataBuilder;
	MVCCStats: enginepb.MVCCStatsBuilder;
	IsolationType: enginepb.IsolationType;
	
}

}

declare module cockroach.storage.engine.enginepb {

	export interface TxnMeta {

		

id?: ByteBuffer;
		

getId?() : ByteBuffer;
		setId?(id : ByteBuffer): void;
		



isolation?: IsolationType;
		

getIsolation?() : IsolationType;
		setIsolation?(isolation : IsolationType): void;
		



key?: ByteBuffer;
		

getKey?() : ByteBuffer;
		setKey?(key : ByteBuffer): void;
		



epoch?: number;
		

getEpoch?() : number;
		setEpoch?(epoch : number): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



priority?: number;
		

getPriority?() : number;
		setPriority?(priority : number): void;
		



sequence?: number;
		

getSequence?() : number;
		setSequence?(sequence : number): void;
		



batch_index?: number;
		

getBatchIndex?() : number;
		setBatchIndex?(batchIndex : number): void;
		



}

	export interface TxnMetaMessage extends TxnMeta {
	add(key: string, value: any, noAssert?: boolean): TxnMetaMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TxnMetaMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TxnMetaBuilder {
	new(data?: TxnMeta): TxnMetaMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TxnMetaMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TxnMetaMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TxnMetaMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TxnMetaMessage;
	decode64(str: string) : TxnMetaMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TxnMetaMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TxnMetaMessage;
	decodeDelimited(buffer: Buffer, enc: string): TxnMetaMessage;
	decodeDelimited(buffer: string, enc: string): TxnMetaMessage;
	decodeHex(str: string): TxnMetaMessage;
	decodeJSON(str: string): TxnMetaMessage;
	
}

}


declare module cockroach.storage.engine.enginepb {

	export interface MVCCMetadata {

		

txn?: TxnMeta;
		

getTxn?() : TxnMeta;
		setTxn?(txn : TxnMeta): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



deleted?: boolean;
		

getDeleted?() : boolean;
		setDeleted?(deleted : boolean): void;
		



key_bytes?: Long;
		

getKeyBytes?() : Long;
		setKeyBytes?(keyBytes : Long): void;
		



val_bytes?: Long;
		

getValBytes?() : Long;
		setValBytes?(valBytes : Long): void;
		



raw_bytes?: ByteBuffer;
		

getRawBytes?() : ByteBuffer;
		setRawBytes?(rawBytes : ByteBuffer): void;
		



merge_timestamp?: util.hlc.Timestamp;
		

getMergeTimestamp?() : util.hlc.Timestamp;
		setMergeTimestamp?(mergeTimestamp : util.hlc.Timestamp): void;
		



}

	export interface MVCCMetadataMessage extends MVCCMetadata {
	add(key: string, value: any, noAssert?: boolean): MVCCMetadataMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): MVCCMetadataMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface MVCCMetadataBuilder {
	new(data?: MVCCMetadata): MVCCMetadataMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : MVCCMetadataMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : MVCCMetadataMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : MVCCMetadataMessage;
	decode(buffer: string, length?: number | string, enc?: string) : MVCCMetadataMessage;
	decode64(str: string) : MVCCMetadataMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): MVCCMetadataMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): MVCCMetadataMessage;
	decodeDelimited(buffer: Buffer, enc: string): MVCCMetadataMessage;
	decodeDelimited(buffer: string, enc: string): MVCCMetadataMessage;
	decodeHex(str: string): MVCCMetadataMessage;
	decodeJSON(str: string): MVCCMetadataMessage;
	
}

}


declare module cockroach.storage.engine.enginepb {

	export interface MVCCStats {

		

contains_estimates?: boolean;
		

getContainsEstimates?() : boolean;
		setContainsEstimates?(containsEstimates : boolean): void;
		



last_update_nanos?: Long;
		

getLastUpdateNanos?() : Long;
		setLastUpdateNanos?(lastUpdateNanos : Long): void;
		



intent_age?: Long;
		

getIntentAge?() : Long;
		setIntentAge?(intentAge : Long): void;
		



gc_bytes_age?: Long;
		

getGcBytesAge?() : Long;
		setGcBytesAge?(gcBytesAge : Long): void;
		



live_bytes?: Long;
		

getLiveBytes?() : Long;
		setLiveBytes?(liveBytes : Long): void;
		



live_count?: Long;
		

getLiveCount?() : Long;
		setLiveCount?(liveCount : Long): void;
		



key_bytes?: Long;
		

getKeyBytes?() : Long;
		setKeyBytes?(keyBytes : Long): void;
		



key_count?: Long;
		

getKeyCount?() : Long;
		setKeyCount?(keyCount : Long): void;
		



val_bytes?: Long;
		

getValBytes?() : Long;
		setValBytes?(valBytes : Long): void;
		



val_count?: Long;
		

getValCount?() : Long;
		setValCount?(valCount : Long): void;
		



intent_bytes?: Long;
		

getIntentBytes?() : Long;
		setIntentBytes?(intentBytes : Long): void;
		



intent_count?: Long;
		

getIntentCount?() : Long;
		setIntentCount?(intentCount : Long): void;
		



sys_bytes?: Long;
		

getSysBytes?() : Long;
		setSysBytes?(sysBytes : Long): void;
		



sys_count?: Long;
		

getSysCount?() : Long;
		setSysCount?(sysCount : Long): void;
		



}

	export interface MVCCStatsMessage extends MVCCStats {
	add(key: string, value: any, noAssert?: boolean): MVCCStatsMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): MVCCStatsMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface MVCCStatsBuilder {
	new(data?: MVCCStats): MVCCStatsMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : MVCCStatsMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : MVCCStatsMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : MVCCStatsMessage;
	decode(buffer: string, length?: number | string, enc?: string) : MVCCStatsMessage;
	decode64(str: string) : MVCCStatsMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): MVCCStatsMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): MVCCStatsMessage;
	decodeDelimited(buffer: Buffer, enc: string): MVCCStatsMessage;
	decodeDelimited(buffer: string, enc: string): MVCCStatsMessage;
	decodeHex(str: string): MVCCStatsMessage;
	decodeJSON(str: string): MVCCStatsMessage;
	
}

}


declare module cockroach.storage.engine.enginepb {
	export const enum IsolationType {
		SERIALIZABLE = 0,
		SNAPSHOT = 1,
		
}
}



declare module cockroach.storage {

	export interface storagebase {

		

}

	export interface storagebaseMessage extends storagebase {
	add(key: string, value: any, noAssert?: boolean): storagebaseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): storagebaseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface storagebaseBuilder {
	new(data?: storagebase): storagebaseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : storagebaseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : storagebaseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : storagebaseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : storagebaseMessage;
	decode64(str: string) : storagebaseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): storagebaseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): storagebaseMessage;
	decodeDelimited(buffer: Buffer, enc: string): storagebaseMessage;
	decodeDelimited(buffer: string, enc: string): storagebaseMessage;
	decodeHex(str: string): storagebaseMessage;
	decodeJSON(str: string): storagebaseMessage;
	ReplicaState: storagebase.ReplicaStateBuilder;
	RangeInfo: storagebase.RangeInfoBuilder;
	
}

}

declare module cockroach.storage.storagebase {

	export interface ReplicaState {

		

raft_applied_index?: Long;
		

getRaftAppliedIndex?() : Long;
		setRaftAppliedIndex?(raftAppliedIndex : Long): void;
		



lease_applied_index?: Long;
		

getLeaseAppliedIndex?() : Long;
		setLeaseAppliedIndex?(leaseAppliedIndex : Long): void;
		



desc?: roachpb.RangeDescriptor;
		

getDesc?() : roachpb.RangeDescriptor;
		setDesc?(desc : roachpb.RangeDescriptor): void;
		



lease?: roachpb.Lease;
		

getLease?() : roachpb.Lease;
		setLease?(lease : roachpb.Lease): void;
		



truncated_state?: roachpb.RaftTruncatedState;
		

getTruncatedState?() : roachpb.RaftTruncatedState;
		setTruncatedState?(truncatedState : roachpb.RaftTruncatedState): void;
		



gc_threshold?: util.hlc.Timestamp;
		

getGcThreshold?() : util.hlc.Timestamp;
		setGcThreshold?(gcThreshold : util.hlc.Timestamp): void;
		



stats?: engine.enginepb.MVCCStats;
		

getStats?() : engine.enginepb.MVCCStats;
		setStats?(stats : engine.enginepb.MVCCStats): void;
		



frozen?: boolean;
		

getFrozen?() : boolean;
		setFrozen?(frozen : boolean): void;
		



}

	export interface ReplicaStateMessage extends ReplicaState {
	add(key: string, value: any, noAssert?: boolean): ReplicaStateMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ReplicaStateMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ReplicaStateBuilder {
	new(data?: ReplicaState): ReplicaStateMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ReplicaStateMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ReplicaStateMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ReplicaStateMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ReplicaStateMessage;
	decode64(str: string) : ReplicaStateMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ReplicaStateMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ReplicaStateMessage;
	decodeDelimited(buffer: Buffer, enc: string): ReplicaStateMessage;
	decodeDelimited(buffer: string, enc: string): ReplicaStateMessage;
	decodeHex(str: string): ReplicaStateMessage;
	decodeJSON(str: string): ReplicaStateMessage;
	
}

}


declare module cockroach.storage.storagebase {

	export interface RangeInfo {

		

state?: ReplicaState;
		

getState?() : ReplicaState;
		setState?(state : ReplicaState): void;
		



lastIndex?: Long;
		

getLastIndex?() : Long;
		setLastIndex?(lastIndex : Long): void;
		



num_pending?: Long;
		

getNumPending?() : Long;
		setNumPending?(numPending : Long): void;
		



last_verification?: util.hlc.Timestamp;
		

getLastVerification?() : util.hlc.Timestamp;
		setLastVerification?(lastVerification : util.hlc.Timestamp): void;
		



num_dropped?: Long;
		

getNumDropped?() : Long;
		setNumDropped?(numDropped : Long): void;
		



raft_log_size?: Long;
		

getRaftLogSize?() : Long;
		setRaftLogSize?(raftLogSize : Long): void;
		



}

	export interface RangeInfoMessage extends RangeInfo {
	add(key: string, value: any, noAssert?: boolean): RangeInfoMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RangeInfoMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RangeInfoBuilder {
	new(data?: RangeInfo): RangeInfoMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RangeInfoMessage;
	decode64(str: string) : RangeInfoMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: Buffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: string, enc: string): RangeInfoMessage;
	decodeHex(str: string): RangeInfoMessage;
	decodeJSON(str: string): RangeInfoMessage;
	
}

}




declare module cockroach {

	export interface config {

		

}

	export interface configMessage extends config {
	add(key: string, value: any, noAssert?: boolean): configMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): configMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface configBuilder {
	new(data?: config): configMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : configMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : configMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : configMessage;
	decode(buffer: string, length?: number | string, enc?: string) : configMessage;
	decode64(str: string) : configMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): configMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): configMessage;
	decodeDelimited(buffer: Buffer, enc: string): configMessage;
	decodeDelimited(buffer: string, enc: string): configMessage;
	decodeHex(str: string): configMessage;
	decodeJSON(str: string): configMessage;
	GCPolicy: config.GCPolicyBuilder;
	ZoneConfig: config.ZoneConfigBuilder;
	SystemConfig: config.SystemConfigBuilder;
	
}

}

declare module cockroach.config {

	export interface GCPolicy {

		

ttl_seconds?: number;
		

getTtlSeconds?() : number;
		setTtlSeconds?(ttlSeconds : number): void;
		



}

	export interface GCPolicyMessage extends GCPolicy {
	add(key: string, value: any, noAssert?: boolean): GCPolicyMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GCPolicyMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GCPolicyBuilder {
	new(data?: GCPolicy): GCPolicyMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GCPolicyMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GCPolicyMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GCPolicyMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GCPolicyMessage;
	decode64(str: string) : GCPolicyMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GCPolicyMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GCPolicyMessage;
	decodeDelimited(buffer: Buffer, enc: string): GCPolicyMessage;
	decodeDelimited(buffer: string, enc: string): GCPolicyMessage;
	decodeHex(str: string): GCPolicyMessage;
	decodeJSON(str: string): GCPolicyMessage;
	
}

}


declare module cockroach.config {

	export interface ZoneConfig {

		

replica_attrs?: roachpb.Attributes[];
		

getReplicaAttrs?() : roachpb.Attributes[];
		setReplicaAttrs?(replicaAttrs : roachpb.Attributes[]): void;
		



range_min_bytes?: Long;
		

getRangeMinBytes?() : Long;
		setRangeMinBytes?(rangeMinBytes : Long): void;
		



range_max_bytes?: Long;
		

getRangeMaxBytes?() : Long;
		setRangeMaxBytes?(rangeMaxBytes : Long): void;
		



gc?: GCPolicy;
		

getGc?() : GCPolicy;
		setGc?(gc : GCPolicy): void;
		



}

	export interface ZoneConfigMessage extends ZoneConfig {
	add(key: string, value: any, noAssert?: boolean): ZoneConfigMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ZoneConfigMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ZoneConfigBuilder {
	new(data?: ZoneConfig): ZoneConfigMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ZoneConfigMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ZoneConfigMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ZoneConfigMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ZoneConfigMessage;
	decode64(str: string) : ZoneConfigMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ZoneConfigMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ZoneConfigMessage;
	decodeDelimited(buffer: Buffer, enc: string): ZoneConfigMessage;
	decodeDelimited(buffer: string, enc: string): ZoneConfigMessage;
	decodeHex(str: string): ZoneConfigMessage;
	decodeJSON(str: string): ZoneConfigMessage;
	
}

}


declare module cockroach.config {

	export interface SystemConfig {

		

values?: roachpb.KeyValue[];
		

getValues?() : roachpb.KeyValue[];
		setValues?(values : roachpb.KeyValue[]): void;
		



}

	export interface SystemConfigMessage extends SystemConfig {
	add(key: string, value: any, noAssert?: boolean): SystemConfigMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SystemConfigMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SystemConfigBuilder {
	new(data?: SystemConfig): SystemConfigMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SystemConfigMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SystemConfigMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SystemConfigMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SystemConfigMessage;
	decode64(str: string) : SystemConfigMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SystemConfigMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SystemConfigMessage;
	decodeDelimited(buffer: Buffer, enc: string): SystemConfigMessage;
	decodeDelimited(buffer: string, enc: string): SystemConfigMessage;
	decodeHex(str: string): SystemConfigMessage;
	decodeJSON(str: string): SystemConfigMessage;
	
}

}



declare module cockroach {

	export interface server {

		

}

	export interface serverMessage extends server {
	add(key: string, value: any, noAssert?: boolean): serverMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): serverMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface serverBuilder {
	new(data?: server): serverMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : serverMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : serverMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : serverMessage;
	decode(buffer: string, length?: number | string, enc?: string) : serverMessage;
	decode64(str: string) : serverMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): serverMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): serverMessage;
	decodeDelimited(buffer: Buffer, enc: string): serverMessage;
	decodeDelimited(buffer: string, enc: string): serverMessage;
	decodeHex(str: string): serverMessage;
	decodeJSON(str: string): serverMessage;
	serverpb: server.serverpbBuilder;
	status: server.statusBuilder;
	
}

}

declare module cockroach.server {

	export interface serverpb {

		

}

	export interface serverpbMessage extends serverpb {
	add(key: string, value: any, noAssert?: boolean): serverpbMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): serverpbMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface serverpbBuilder {
	new(data?: serverpb): serverpbMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : serverpbMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : serverpbMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : serverpbMessage;
	decode(buffer: string, length?: number | string, enc?: string) : serverpbMessage;
	decode64(str: string) : serverpbMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): serverpbMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): serverpbMessage;
	decodeDelimited(buffer: Buffer, enc: string): serverpbMessage;
	decodeDelimited(buffer: string, enc: string): serverpbMessage;
	decodeHex(str: string): serverpbMessage;
	decodeJSON(str: string): serverpbMessage;
	DatabasesRequest: serverpb.DatabasesRequestBuilder;
	DatabasesResponse: serverpb.DatabasesResponseBuilder;
	DatabaseDetailsRequest: serverpb.DatabaseDetailsRequestBuilder;
	DatabaseDetailsResponse: serverpb.DatabaseDetailsResponseBuilder;
	TableDetailsRequest: serverpb.TableDetailsRequestBuilder;
	TableDetailsResponse: serverpb.TableDetailsResponseBuilder;
	TableStatsRequest: serverpb.TableStatsRequestBuilder;
	TableStatsResponse: serverpb.TableStatsResponseBuilder;
	UsersRequest: serverpb.UsersRequestBuilder;
	UsersResponse: serverpb.UsersResponseBuilder;
	EventsRequest: serverpb.EventsRequestBuilder;
	EventsResponse: serverpb.EventsResponseBuilder;
	SetUIDataRequest: serverpb.SetUIDataRequestBuilder;
	SetUIDataResponse: serverpb.SetUIDataResponseBuilder;
	GetUIDataRequest: serverpb.GetUIDataRequestBuilder;
	GetUIDataResponse: serverpb.GetUIDataResponseBuilder;
	ClusterRequest: serverpb.ClusterRequestBuilder;
	ClusterResponse: serverpb.ClusterResponseBuilder;
	DrainRequest: serverpb.DrainRequestBuilder;
	DrainResponse: serverpb.DrainResponseBuilder;
	HealthRequest: serverpb.HealthRequestBuilder;
	HealthResponse: serverpb.HealthResponseBuilder;
	ClusterFreezeRequest: serverpb.ClusterFreezeRequestBuilder;
	ClusterFreezeResponse: serverpb.ClusterFreezeResponseBuilder;
	DetailsRequest: serverpb.DetailsRequestBuilder;
	DetailsResponse: serverpb.DetailsResponseBuilder;
	NodesRequest: serverpb.NodesRequestBuilder;
	NodesResponse: serverpb.NodesResponseBuilder;
	NodeRequest: serverpb.NodeRequestBuilder;
	RangeInfo: serverpb.RangeInfoBuilder;
	RangesRequest: serverpb.RangesRequestBuilder;
	RangesResponse: serverpb.RangesResponseBuilder;
	GossipRequest: serverpb.GossipRequestBuilder;
	JSONResponse: serverpb.JSONResponseBuilder;
	LogsRequest: serverpb.LogsRequestBuilder;
	LogFilesListRequest: serverpb.LogFilesListRequestBuilder;
	LogFileRequest: serverpb.LogFileRequestBuilder;
	StacksRequest: serverpb.StacksRequestBuilder;
	MetricsRequest: serverpb.MetricsRequestBuilder;
	RaftRangeNode: serverpb.RaftRangeNodeBuilder;
	RaftRangeError: serverpb.RaftRangeErrorBuilder;
	RaftRangeStatus: serverpb.RaftRangeStatusBuilder;
	RaftDebugRequest: serverpb.RaftDebugRequestBuilder;
	RaftDebugResponse: serverpb.RaftDebugResponseBuilder;
	SpanStatsRequest: serverpb.SpanStatsRequestBuilder;
	SpanStatsResponse: serverpb.SpanStatsResponseBuilder;
	PrettySpan: serverpb.PrettySpanBuilder;
	ZoneConfigurationLevel: serverpb.ZoneConfigurationLevel;
	DrainMode: serverpb.DrainMode;
	
}

}

declare module cockroach.server.serverpb {

	export interface DatabasesRequest {

		

}

	export interface DatabasesRequestMessage extends DatabasesRequest {
	add(key: string, value: any, noAssert?: boolean): DatabasesRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DatabasesRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DatabasesRequestBuilder {
	new(data?: DatabasesRequest): DatabasesRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DatabasesRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DatabasesRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DatabasesRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DatabasesRequestMessage;
	decode64(str: string) : DatabasesRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DatabasesRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DatabasesRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): DatabasesRequestMessage;
	decodeDelimited(buffer: string, enc: string): DatabasesRequestMessage;
	decodeHex(str: string): DatabasesRequestMessage;
	decodeJSON(str: string): DatabasesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DatabasesResponse {

		

databases?: string[];
		

getDatabases?() : string[];
		setDatabases?(databases : string[]): void;
		



}

	export interface DatabasesResponseMessage extends DatabasesResponse {
	add(key: string, value: any, noAssert?: boolean): DatabasesResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DatabasesResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DatabasesResponseBuilder {
	new(data?: DatabasesResponse): DatabasesResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DatabasesResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DatabasesResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DatabasesResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DatabasesResponseMessage;
	decode64(str: string) : DatabasesResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DatabasesResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DatabasesResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): DatabasesResponseMessage;
	decodeDelimited(buffer: string, enc: string): DatabasesResponseMessage;
	decodeHex(str: string): DatabasesResponseMessage;
	decodeJSON(str: string): DatabasesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DatabaseDetailsRequest {

		

database?: string;
		

getDatabase?() : string;
		setDatabase?(database : string): void;
		



}

	export interface DatabaseDetailsRequestMessage extends DatabaseDetailsRequest {
	add(key: string, value: any, noAssert?: boolean): DatabaseDetailsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DatabaseDetailsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DatabaseDetailsRequestBuilder {
	new(data?: DatabaseDetailsRequest): DatabaseDetailsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DatabaseDetailsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DatabaseDetailsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DatabaseDetailsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DatabaseDetailsRequestMessage;
	decode64(str: string) : DatabaseDetailsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DatabaseDetailsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DatabaseDetailsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): DatabaseDetailsRequestMessage;
	decodeDelimited(buffer: string, enc: string): DatabaseDetailsRequestMessage;
	decodeHex(str: string): DatabaseDetailsRequestMessage;
	decodeJSON(str: string): DatabaseDetailsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DatabaseDetailsResponse {

		

grants?: DatabaseDetailsResponse.Grant[];
		

getGrants?() : DatabaseDetailsResponse.Grant[];
		setGrants?(grants : DatabaseDetailsResponse.Grant[]): void;
		



table_names?: string[];
		

getTableNames?() : string[];
		setTableNames?(tableNames : string[]): void;
		



}

	export interface DatabaseDetailsResponseMessage extends DatabaseDetailsResponse {
	add(key: string, value: any, noAssert?: boolean): DatabaseDetailsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DatabaseDetailsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DatabaseDetailsResponseBuilder {
	new(data?: DatabaseDetailsResponse): DatabaseDetailsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DatabaseDetailsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DatabaseDetailsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DatabaseDetailsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DatabaseDetailsResponseMessage;
	decode64(str: string) : DatabaseDetailsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DatabaseDetailsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DatabaseDetailsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): DatabaseDetailsResponseMessage;
	decodeDelimited(buffer: string, enc: string): DatabaseDetailsResponseMessage;
	decodeHex(str: string): DatabaseDetailsResponseMessage;
	decodeJSON(str: string): DatabaseDetailsResponseMessage;
	Grant: DatabaseDetailsResponse.GrantBuilder;
	
}

}

declare module cockroach.server.serverpb.DatabaseDetailsResponse {

	export interface Grant {

		

user?: string;
		

getUser?() : string;
		setUser?(user : string): void;
		



privileges?: string[];
		

getPrivileges?() : string[];
		setPrivileges?(privileges : string[]): void;
		



}

	export interface GrantMessage extends Grant {
	add(key: string, value: any, noAssert?: boolean): GrantMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GrantMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GrantBuilder {
	new(data?: Grant): GrantMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GrantMessage;
	decode64(str: string) : GrantMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GrantMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GrantMessage;
	decodeDelimited(buffer: Buffer, enc: string): GrantMessage;
	decodeDelimited(buffer: string, enc: string): GrantMessage;
	decodeHex(str: string): GrantMessage;
	decodeJSON(str: string): GrantMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface TableDetailsRequest {

		

database?: string;
		

getDatabase?() : string;
		setDatabase?(database : string): void;
		



table?: string;
		

getTable?() : string;
		setTable?(table : string): void;
		



}

	export interface TableDetailsRequestMessage extends TableDetailsRequest {
	add(key: string, value: any, noAssert?: boolean): TableDetailsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TableDetailsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TableDetailsRequestBuilder {
	new(data?: TableDetailsRequest): TableDetailsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TableDetailsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TableDetailsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TableDetailsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TableDetailsRequestMessage;
	decode64(str: string) : TableDetailsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TableDetailsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TableDetailsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): TableDetailsRequestMessage;
	decodeDelimited(buffer: string, enc: string): TableDetailsRequestMessage;
	decodeHex(str: string): TableDetailsRequestMessage;
	decodeJSON(str: string): TableDetailsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface TableDetailsResponse {

		

grants?: TableDetailsResponse.Grant[];
		

getGrants?() : TableDetailsResponse.Grant[];
		setGrants?(grants : TableDetailsResponse.Grant[]): void;
		



columns?: TableDetailsResponse.Column[];
		

getColumns?() : TableDetailsResponse.Column[];
		setColumns?(columns : TableDetailsResponse.Column[]): void;
		



indexes?: TableDetailsResponse.Index[];
		

getIndexes?() : TableDetailsResponse.Index[];
		setIndexes?(indexes : TableDetailsResponse.Index[]): void;
		



range_count?: Long;
		

getRangeCount?() : Long;
		setRangeCount?(rangeCount : Long): void;
		



create_table_statement?: string;
		

getCreateTableStatement?() : string;
		setCreateTableStatement?(createTableStatement : string): void;
		



zone_config?: config.ZoneConfig;
		

getZoneConfig?() : config.ZoneConfig;
		setZoneConfig?(zoneConfig : config.ZoneConfig): void;
		



zone_config_level?: ZoneConfigurationLevel;
		

getZoneConfigLevel?() : ZoneConfigurationLevel;
		setZoneConfigLevel?(zoneConfigLevel : ZoneConfigurationLevel): void;
		



}

	export interface TableDetailsResponseMessage extends TableDetailsResponse {
	add(key: string, value: any, noAssert?: boolean): TableDetailsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TableDetailsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TableDetailsResponseBuilder {
	new(data?: TableDetailsResponse): TableDetailsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TableDetailsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TableDetailsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TableDetailsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TableDetailsResponseMessage;
	decode64(str: string) : TableDetailsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TableDetailsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TableDetailsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): TableDetailsResponseMessage;
	decodeDelimited(buffer: string, enc: string): TableDetailsResponseMessage;
	decodeHex(str: string): TableDetailsResponseMessage;
	decodeJSON(str: string): TableDetailsResponseMessage;
	Grant: TableDetailsResponse.GrantBuilder;
	Column: TableDetailsResponse.ColumnBuilder;
	Index: TableDetailsResponse.IndexBuilder;
	
}

}

declare module cockroach.server.serverpb.TableDetailsResponse {

	export interface Grant {

		

user?: string;
		

getUser?() : string;
		setUser?(user : string): void;
		



privileges?: string[];
		

getPrivileges?() : string[];
		setPrivileges?(privileges : string[]): void;
		



}

	export interface GrantMessage extends Grant {
	add(key: string, value: any, noAssert?: boolean): GrantMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GrantMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GrantBuilder {
	new(data?: Grant): GrantMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GrantMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GrantMessage;
	decode64(str: string) : GrantMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GrantMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GrantMessage;
	decodeDelimited(buffer: Buffer, enc: string): GrantMessage;
	decodeDelimited(buffer: string, enc: string): GrantMessage;
	decodeHex(str: string): GrantMessage;
	decodeJSON(str: string): GrantMessage;
	
}

}


declare module cockroach.server.serverpb.TableDetailsResponse {

	export interface Column {

		

name?: string;
		

getName?() : string;
		setName?(name : string): void;
		



type?: string;
		

getType?() : string;
		setType?(type : string): void;
		



nullable?: boolean;
		

getNullable?() : boolean;
		setNullable?(nullable : boolean): void;
		



default_value?: string;
		

getDefaultValue?() : string;
		setDefaultValue?(defaultValue : string): void;
		



}

	export interface ColumnMessage extends Column {
	add(key: string, value: any, noAssert?: boolean): ColumnMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ColumnMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ColumnBuilder {
	new(data?: Column): ColumnMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ColumnMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ColumnMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ColumnMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ColumnMessage;
	decode64(str: string) : ColumnMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ColumnMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ColumnMessage;
	decodeDelimited(buffer: Buffer, enc: string): ColumnMessage;
	decodeDelimited(buffer: string, enc: string): ColumnMessage;
	decodeHex(str: string): ColumnMessage;
	decodeJSON(str: string): ColumnMessage;
	
}

}


declare module cockroach.server.serverpb.TableDetailsResponse {

	export interface Index {

		

name?: string;
		

getName?() : string;
		setName?(name : string): void;
		



unique?: boolean;
		

getUnique?() : boolean;
		setUnique?(unique : boolean): void;
		



seq?: Long;
		

getSeq?() : Long;
		setSeq?(seq : Long): void;
		



column?: string;
		

getColumn?() : string;
		setColumn?(column : string): void;
		



direction?: string;
		

getDirection?() : string;
		setDirection?(direction : string): void;
		



storing?: boolean;
		

getStoring?() : boolean;
		setStoring?(storing : boolean): void;
		



}

	export interface IndexMessage extends Index {
	add(key: string, value: any, noAssert?: boolean): IndexMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): IndexMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface IndexBuilder {
	new(data?: Index): IndexMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : IndexMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : IndexMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : IndexMessage;
	decode(buffer: string, length?: number | string, enc?: string) : IndexMessage;
	decode64(str: string) : IndexMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): IndexMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): IndexMessage;
	decodeDelimited(buffer: Buffer, enc: string): IndexMessage;
	decodeDelimited(buffer: string, enc: string): IndexMessage;
	decodeHex(str: string): IndexMessage;
	decodeJSON(str: string): IndexMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface TableStatsRequest {

		

database?: string;
		

getDatabase?() : string;
		setDatabase?(database : string): void;
		



table?: string;
		

getTable?() : string;
		setTable?(table : string): void;
		



}

	export interface TableStatsRequestMessage extends TableStatsRequest {
	add(key: string, value: any, noAssert?: boolean): TableStatsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TableStatsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TableStatsRequestBuilder {
	new(data?: TableStatsRequest): TableStatsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TableStatsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TableStatsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TableStatsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TableStatsRequestMessage;
	decode64(str: string) : TableStatsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TableStatsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TableStatsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): TableStatsRequestMessage;
	decodeDelimited(buffer: string, enc: string): TableStatsRequestMessage;
	decodeHex(str: string): TableStatsRequestMessage;
	decodeJSON(str: string): TableStatsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface TableStatsResponse {

		

range_count?: Long;
		

getRangeCount?() : Long;
		setRangeCount?(rangeCount : Long): void;
		



replica_count?: Long;
		

getReplicaCount?() : Long;
		setReplicaCount?(replicaCount : Long): void;
		



node_count?: Long;
		

getNodeCount?() : Long;
		setNodeCount?(nodeCount : Long): void;
		



stats?: storage.engine.enginepb.MVCCStats;
		

getStats?() : storage.engine.enginepb.MVCCStats;
		setStats?(stats : storage.engine.enginepb.MVCCStats): void;
		



missing_nodes?: TableStatsResponse.MissingNode[];
		

getMissingNodes?() : TableStatsResponse.MissingNode[];
		setMissingNodes?(missingNodes : TableStatsResponse.MissingNode[]): void;
		



}

	export interface TableStatsResponseMessage extends TableStatsResponse {
	add(key: string, value: any, noAssert?: boolean): TableStatsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TableStatsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TableStatsResponseBuilder {
	new(data?: TableStatsResponse): TableStatsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TableStatsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TableStatsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TableStatsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TableStatsResponseMessage;
	decode64(str: string) : TableStatsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TableStatsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TableStatsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): TableStatsResponseMessage;
	decodeDelimited(buffer: string, enc: string): TableStatsResponseMessage;
	decodeHex(str: string): TableStatsResponseMessage;
	decodeJSON(str: string): TableStatsResponseMessage;
	MissingNode: TableStatsResponse.MissingNodeBuilder;
	
}

}

declare module cockroach.server.serverpb.TableStatsResponse {

	export interface MissingNode {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



error_message?: string;
		

getErrorMessage?() : string;
		setErrorMessage?(errorMessage : string): void;
		



}

	export interface MissingNodeMessage extends MissingNode {
	add(key: string, value: any, noAssert?: boolean): MissingNodeMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): MissingNodeMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface MissingNodeBuilder {
	new(data?: MissingNode): MissingNodeMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : MissingNodeMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : MissingNodeMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : MissingNodeMessage;
	decode(buffer: string, length?: number | string, enc?: string) : MissingNodeMessage;
	decode64(str: string) : MissingNodeMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): MissingNodeMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): MissingNodeMessage;
	decodeDelimited(buffer: Buffer, enc: string): MissingNodeMessage;
	decodeDelimited(buffer: string, enc: string): MissingNodeMessage;
	decodeHex(str: string): MissingNodeMessage;
	decodeJSON(str: string): MissingNodeMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface UsersRequest {

		

}

	export interface UsersRequestMessage extends UsersRequest {
	add(key: string, value: any, noAssert?: boolean): UsersRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): UsersRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface UsersRequestBuilder {
	new(data?: UsersRequest): UsersRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : UsersRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : UsersRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : UsersRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : UsersRequestMessage;
	decode64(str: string) : UsersRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): UsersRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): UsersRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): UsersRequestMessage;
	decodeDelimited(buffer: string, enc: string): UsersRequestMessage;
	decodeHex(str: string): UsersRequestMessage;
	decodeJSON(str: string): UsersRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface UsersResponse {

		

users?: UsersResponse.User[];
		

getUsers?() : UsersResponse.User[];
		setUsers?(users : UsersResponse.User[]): void;
		



}

	export interface UsersResponseMessage extends UsersResponse {
	add(key: string, value: any, noAssert?: boolean): UsersResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): UsersResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface UsersResponseBuilder {
	new(data?: UsersResponse): UsersResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : UsersResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : UsersResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : UsersResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : UsersResponseMessage;
	decode64(str: string) : UsersResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): UsersResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): UsersResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): UsersResponseMessage;
	decodeDelimited(buffer: string, enc: string): UsersResponseMessage;
	decodeHex(str: string): UsersResponseMessage;
	decodeJSON(str: string): UsersResponseMessage;
	User: UsersResponse.UserBuilder;
	
}

}

declare module cockroach.server.serverpb.UsersResponse {

	export interface User {

		

username?: string;
		

getUsername?() : string;
		setUsername?(username : string): void;
		



}

	export interface UserMessage extends User {
	add(key: string, value: any, noAssert?: boolean): UserMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): UserMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface UserBuilder {
	new(data?: User): UserMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : UserMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : UserMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : UserMessage;
	decode(buffer: string, length?: number | string, enc?: string) : UserMessage;
	decode64(str: string) : UserMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): UserMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): UserMessage;
	decodeDelimited(buffer: Buffer, enc: string): UserMessage;
	decodeDelimited(buffer: string, enc: string): UserMessage;
	decodeHex(str: string): UserMessage;
	decodeJSON(str: string): UserMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface EventsRequest {

		

type?: string;
		

getType?() : string;
		setType?(type : string): void;
		



target_id?: Long;
		

getTargetId?() : Long;
		setTargetId?(targetId : Long): void;
		



}

	export interface EventsRequestMessage extends EventsRequest {
	add(key: string, value: any, noAssert?: boolean): EventsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): EventsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface EventsRequestBuilder {
	new(data?: EventsRequest): EventsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : EventsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : EventsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : EventsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : EventsRequestMessage;
	decode64(str: string) : EventsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): EventsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): EventsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): EventsRequestMessage;
	decodeDelimited(buffer: string, enc: string): EventsRequestMessage;
	decodeHex(str: string): EventsRequestMessage;
	decodeJSON(str: string): EventsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface EventsResponse {

		

events?: EventsResponse.Event[];
		

getEvents?() : EventsResponse.Event[];
		setEvents?(events : EventsResponse.Event[]): void;
		



}

	export interface EventsResponseMessage extends EventsResponse {
	add(key: string, value: any, noAssert?: boolean): EventsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): EventsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface EventsResponseBuilder {
	new(data?: EventsResponse): EventsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : EventsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : EventsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : EventsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : EventsResponseMessage;
	decode64(str: string) : EventsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): EventsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): EventsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): EventsResponseMessage;
	decodeDelimited(buffer: string, enc: string): EventsResponseMessage;
	decodeHex(str: string): EventsResponseMessage;
	decodeJSON(str: string): EventsResponseMessage;
	Event: EventsResponse.EventBuilder;
	
}

}

declare module cockroach.server.serverpb.EventsResponse {

	export interface Event {

		

timestamp?: Event.Timestamp;
		

getTimestamp?() : Event.Timestamp;
		setTimestamp?(timestamp : Event.Timestamp): void;
		



event_type?: string;
		

getEventType?() : string;
		setEventType?(eventType : string): void;
		



target_id?: Long;
		

getTargetId?() : Long;
		setTargetId?(targetId : Long): void;
		



reporting_id?: Long;
		

getReportingId?() : Long;
		setReportingId?(reportingId : Long): void;
		



info?: string;
		

getInfo?() : string;
		setInfo?(info : string): void;
		



unique_id?: ByteBuffer;
		

getUniqueId?() : ByteBuffer;
		setUniqueId?(uniqueId : ByteBuffer): void;
		



}

	export interface EventMessage extends Event {
	add(key: string, value: any, noAssert?: boolean): EventMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): EventMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface EventBuilder {
	new(data?: Event): EventMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : EventMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : EventMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : EventMessage;
	decode(buffer: string, length?: number | string, enc?: string) : EventMessage;
	decode64(str: string) : EventMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): EventMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): EventMessage;
	decodeDelimited(buffer: Buffer, enc: string): EventMessage;
	decodeDelimited(buffer: string, enc: string): EventMessage;
	decodeHex(str: string): EventMessage;
	decodeJSON(str: string): EventMessage;
	Timestamp: Event.TimestampBuilder;
	
}

}

declare module cockroach.server.serverpb.EventsResponse.Event {

	export interface Timestamp {

		

sec?: Long;
		

getSec?() : Long;
		setSec?(sec : Long): void;
		



nsec?: number;
		

getNsec?() : number;
		setNsec?(nsec : number): void;
		



}

	export interface TimestampMessage extends Timestamp {
	add(key: string, value: any, noAssert?: boolean): TimestampMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimestampMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimestampMessage;
	decode64(str: string) : TimestampMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: string, enc: string): TimestampMessage;
	decodeHex(str: string): TimestampMessage;
	decodeJSON(str: string): TimestampMessage;
	
}

}




declare module cockroach.server.serverpb {

	export interface SetUIDataRequest {

		

key_values?: ProtoBufMap<string, ByteBuffer>;
		

getKeyValues?() : ProtoBufMap<string, ByteBuffer>;
		setKeyValues?(keyValues : ProtoBufMap<string, ByteBuffer>): void;
		



}

	export interface SetUIDataRequestMessage extends SetUIDataRequest {
	add(key: string, value: any, noAssert?: boolean): SetUIDataRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SetUIDataRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SetUIDataRequestBuilder {
	new(data?: SetUIDataRequest): SetUIDataRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SetUIDataRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SetUIDataRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SetUIDataRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SetUIDataRequestMessage;
	decode64(str: string) : SetUIDataRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SetUIDataRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SetUIDataRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): SetUIDataRequestMessage;
	decodeDelimited(buffer: string, enc: string): SetUIDataRequestMessage;
	decodeHex(str: string): SetUIDataRequestMessage;
	decodeJSON(str: string): SetUIDataRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface SetUIDataResponse {

		

}

	export interface SetUIDataResponseMessage extends SetUIDataResponse {
	add(key: string, value: any, noAssert?: boolean): SetUIDataResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SetUIDataResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SetUIDataResponseBuilder {
	new(data?: SetUIDataResponse): SetUIDataResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SetUIDataResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SetUIDataResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SetUIDataResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SetUIDataResponseMessage;
	decode64(str: string) : SetUIDataResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SetUIDataResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SetUIDataResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): SetUIDataResponseMessage;
	decodeDelimited(buffer: string, enc: string): SetUIDataResponseMessage;
	decodeHex(str: string): SetUIDataResponseMessage;
	decodeJSON(str: string): SetUIDataResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GetUIDataRequest {

		

keys?: string[];
		

getKeys?() : string[];
		setKeys?(keys : string[]): void;
		



}

	export interface GetUIDataRequestMessage extends GetUIDataRequest {
	add(key: string, value: any, noAssert?: boolean): GetUIDataRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GetUIDataRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GetUIDataRequestBuilder {
	new(data?: GetUIDataRequest): GetUIDataRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GetUIDataRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GetUIDataRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GetUIDataRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GetUIDataRequestMessage;
	decode64(str: string) : GetUIDataRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GetUIDataRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GetUIDataRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): GetUIDataRequestMessage;
	decodeDelimited(buffer: string, enc: string): GetUIDataRequestMessage;
	decodeHex(str: string): GetUIDataRequestMessage;
	decodeJSON(str: string): GetUIDataRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GetUIDataResponse {

		

key_values?: ProtoBufMap<string, GetUIDataResponse.Value>;
		

getKeyValues?() : ProtoBufMap<string, GetUIDataResponse.Value>;
		setKeyValues?(keyValues : ProtoBufMap<string, GetUIDataResponse.Value>): void;
		



}

	export interface GetUIDataResponseMessage extends GetUIDataResponse {
	add(key: string, value: any, noAssert?: boolean): GetUIDataResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GetUIDataResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GetUIDataResponseBuilder {
	new(data?: GetUIDataResponse): GetUIDataResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GetUIDataResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GetUIDataResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GetUIDataResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GetUIDataResponseMessage;
	decode64(str: string) : GetUIDataResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GetUIDataResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GetUIDataResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): GetUIDataResponseMessage;
	decodeDelimited(buffer: string, enc: string): GetUIDataResponseMessage;
	decodeHex(str: string): GetUIDataResponseMessage;
	decodeJSON(str: string): GetUIDataResponseMessage;
	Timestamp: GetUIDataResponse.TimestampBuilder;
	Value: GetUIDataResponse.ValueBuilder;
	
}

}

declare module cockroach.server.serverpb.GetUIDataResponse {

	export interface Timestamp {

		

sec?: Long;
		

getSec?() : Long;
		setSec?(sec : Long): void;
		



nsec?: number;
		

getNsec?() : number;
		setNsec?(nsec : number): void;
		



}

	export interface TimestampMessage extends Timestamp {
	add(key: string, value: any, noAssert?: boolean): TimestampMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimestampMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimestampMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimestampMessage;
	decode64(str: string) : TimestampMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimestampMessage;
	decodeDelimited(buffer: string, enc: string): TimestampMessage;
	decodeHex(str: string): TimestampMessage;
	decodeJSON(str: string): TimestampMessage;
	
}

}


declare module cockroach.server.serverpb.GetUIDataResponse {

	export interface Value {

		

value?: ByteBuffer;
		

getValue?() : ByteBuffer;
		setValue?(value : ByteBuffer): void;
		



last_updated?: Timestamp;
		

getLastUpdated?() : Timestamp;
		setLastUpdated?(lastUpdated : Timestamp): void;
		



}

	export interface ValueMessage extends Value {
	add(key: string, value: any, noAssert?: boolean): ValueMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ValueMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ValueBuilder {
	new(data?: Value): ValueMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ValueMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ValueMessage;
	decode64(str: string) : ValueMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ValueMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ValueMessage;
	decodeDelimited(buffer: Buffer, enc: string): ValueMessage;
	decodeDelimited(buffer: string, enc: string): ValueMessage;
	decodeHex(str: string): ValueMessage;
	decodeJSON(str: string): ValueMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface ClusterRequest {

		

}

	export interface ClusterRequestMessage extends ClusterRequest {
	add(key: string, value: any, noAssert?: boolean): ClusterRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ClusterRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ClusterRequestBuilder {
	new(data?: ClusterRequest): ClusterRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ClusterRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ClusterRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ClusterRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ClusterRequestMessage;
	decode64(str: string) : ClusterRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ClusterRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ClusterRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): ClusterRequestMessage;
	decodeDelimited(buffer: string, enc: string): ClusterRequestMessage;
	decodeHex(str: string): ClusterRequestMessage;
	decodeJSON(str: string): ClusterRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface ClusterResponse {

		

cluster_id?: string;
		

getClusterId?() : string;
		setClusterId?(clusterId : string): void;
		



}

	export interface ClusterResponseMessage extends ClusterResponse {
	add(key: string, value: any, noAssert?: boolean): ClusterResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ClusterResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ClusterResponseBuilder {
	new(data?: ClusterResponse): ClusterResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ClusterResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ClusterResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ClusterResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ClusterResponseMessage;
	decode64(str: string) : ClusterResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ClusterResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ClusterResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): ClusterResponseMessage;
	decodeDelimited(buffer: string, enc: string): ClusterResponseMessage;
	decodeHex(str: string): ClusterResponseMessage;
	decodeJSON(str: string): ClusterResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DrainRequest {

		

on?: number[];
		

getOn?() : number[];
		setOn?(on : number[]): void;
		



off?: number[];
		

getOff?() : number[];
		setOff?(off : number[]): void;
		



shutdown?: boolean;
		

getShutdown?() : boolean;
		setShutdown?(shutdown : boolean): void;
		



}

	export interface DrainRequestMessage extends DrainRequest {
	add(key: string, value: any, noAssert?: boolean): DrainRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DrainRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DrainRequestBuilder {
	new(data?: DrainRequest): DrainRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DrainRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DrainRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DrainRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DrainRequestMessage;
	decode64(str: string) : DrainRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DrainRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DrainRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): DrainRequestMessage;
	decodeDelimited(buffer: string, enc: string): DrainRequestMessage;
	decodeHex(str: string): DrainRequestMessage;
	decodeJSON(str: string): DrainRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DrainResponse {

		

on?: number[];
		

getOn?() : number[];
		setOn?(on : number[]): void;
		



}

	export interface DrainResponseMessage extends DrainResponse {
	add(key: string, value: any, noAssert?: boolean): DrainResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DrainResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DrainResponseBuilder {
	new(data?: DrainResponse): DrainResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DrainResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DrainResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DrainResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DrainResponseMessage;
	decode64(str: string) : DrainResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DrainResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DrainResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): DrainResponseMessage;
	decodeDelimited(buffer: string, enc: string): DrainResponseMessage;
	decodeHex(str: string): DrainResponseMessage;
	decodeJSON(str: string): DrainResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface HealthRequest {

		

}

	export interface HealthRequestMessage extends HealthRequest {
	add(key: string, value: any, noAssert?: boolean): HealthRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): HealthRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface HealthRequestBuilder {
	new(data?: HealthRequest): HealthRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : HealthRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : HealthRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : HealthRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : HealthRequestMessage;
	decode64(str: string) : HealthRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): HealthRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): HealthRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): HealthRequestMessage;
	decodeDelimited(buffer: string, enc: string): HealthRequestMessage;
	decodeHex(str: string): HealthRequestMessage;
	decodeJSON(str: string): HealthRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface HealthResponse {

		

}

	export interface HealthResponseMessage extends HealthResponse {
	add(key: string, value: any, noAssert?: boolean): HealthResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): HealthResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface HealthResponseBuilder {
	new(data?: HealthResponse): HealthResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : HealthResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : HealthResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : HealthResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : HealthResponseMessage;
	decode64(str: string) : HealthResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): HealthResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): HealthResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): HealthResponseMessage;
	decodeDelimited(buffer: string, enc: string): HealthResponseMessage;
	decodeHex(str: string): HealthResponseMessage;
	decodeJSON(str: string): HealthResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface ClusterFreezeRequest {

		

freeze?: boolean;
		

getFreeze?() : boolean;
		setFreeze?(freeze : boolean): void;
		



}

	export interface ClusterFreezeRequestMessage extends ClusterFreezeRequest {
	add(key: string, value: any, noAssert?: boolean): ClusterFreezeRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ClusterFreezeRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ClusterFreezeRequestBuilder {
	new(data?: ClusterFreezeRequest): ClusterFreezeRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ClusterFreezeRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ClusterFreezeRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ClusterFreezeRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ClusterFreezeRequestMessage;
	decode64(str: string) : ClusterFreezeRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ClusterFreezeRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ClusterFreezeRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): ClusterFreezeRequestMessage;
	decodeDelimited(buffer: string, enc: string): ClusterFreezeRequestMessage;
	decodeHex(str: string): ClusterFreezeRequestMessage;
	decodeJSON(str: string): ClusterFreezeRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface ClusterFreezeResponse {

		

ranges_affected?: Long;
		

getRangesAffected?() : Long;
		setRangesAffected?(rangesAffected : Long): void;
		



message?: string;
		

getMessage?() : string;
		setMessage?(message : string): void;
		



}

	export interface ClusterFreezeResponseMessage extends ClusterFreezeResponse {
	add(key: string, value: any, noAssert?: boolean): ClusterFreezeResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ClusterFreezeResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ClusterFreezeResponseBuilder {
	new(data?: ClusterFreezeResponse): ClusterFreezeResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ClusterFreezeResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ClusterFreezeResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ClusterFreezeResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ClusterFreezeResponseMessage;
	decode64(str: string) : ClusterFreezeResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ClusterFreezeResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ClusterFreezeResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): ClusterFreezeResponseMessage;
	decodeDelimited(buffer: string, enc: string): ClusterFreezeResponseMessage;
	decodeHex(str: string): ClusterFreezeResponseMessage;
	decodeJSON(str: string): ClusterFreezeResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DetailsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface DetailsRequestMessage extends DetailsRequest {
	add(key: string, value: any, noAssert?: boolean): DetailsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DetailsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DetailsRequestBuilder {
	new(data?: DetailsRequest): DetailsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DetailsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DetailsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DetailsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DetailsRequestMessage;
	decode64(str: string) : DetailsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DetailsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DetailsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): DetailsRequestMessage;
	decodeDelimited(buffer: string, enc: string): DetailsRequestMessage;
	decodeHex(str: string): DetailsRequestMessage;
	decodeJSON(str: string): DetailsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DetailsResponse {

		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



address?: util.UnresolvedAddr;
		

getAddress?() : util.UnresolvedAddr;
		setAddress?(address : util.UnresolvedAddr): void;
		



build_info?: build.Info;
		

getBuildInfo?() : build.Info;
		setBuildInfo?(buildInfo : build.Info): void;
		



}

	export interface DetailsResponseMessage extends DetailsResponse {
	add(key: string, value: any, noAssert?: boolean): DetailsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): DetailsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface DetailsResponseBuilder {
	new(data?: DetailsResponse): DetailsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : DetailsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : DetailsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : DetailsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : DetailsResponseMessage;
	decode64(str: string) : DetailsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): DetailsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): DetailsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): DetailsResponseMessage;
	decodeDelimited(buffer: string, enc: string): DetailsResponseMessage;
	decodeHex(str: string): DetailsResponseMessage;
	decodeJSON(str: string): DetailsResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodesRequest {

		

}

	export interface NodesRequestMessage extends NodesRequest {
	add(key: string, value: any, noAssert?: boolean): NodesRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): NodesRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface NodesRequestBuilder {
	new(data?: NodesRequest): NodesRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : NodesRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : NodesRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : NodesRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : NodesRequestMessage;
	decode64(str: string) : NodesRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): NodesRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): NodesRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): NodesRequestMessage;
	decodeDelimited(buffer: string, enc: string): NodesRequestMessage;
	decodeHex(str: string): NodesRequestMessage;
	decodeJSON(str: string): NodesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodesResponse {

		

nodes?: status.NodeStatus[];
		

getNodes?() : status.NodeStatus[];
		setNodes?(nodes : status.NodeStatus[]): void;
		



}

	export interface NodesResponseMessage extends NodesResponse {
	add(key: string, value: any, noAssert?: boolean): NodesResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): NodesResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface NodesResponseBuilder {
	new(data?: NodesResponse): NodesResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : NodesResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : NodesResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : NodesResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : NodesResponseMessage;
	decode64(str: string) : NodesResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): NodesResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): NodesResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): NodesResponseMessage;
	decodeDelimited(buffer: string, enc: string): NodesResponseMessage;
	decodeHex(str: string): NodesResponseMessage;
	decodeJSON(str: string): NodesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodeRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface NodeRequestMessage extends NodeRequest {
	add(key: string, value: any, noAssert?: boolean): NodeRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): NodeRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface NodeRequestBuilder {
	new(data?: NodeRequest): NodeRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : NodeRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : NodeRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : NodeRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : NodeRequestMessage;
	decode64(str: string) : NodeRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): NodeRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): NodeRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): NodeRequestMessage;
	decodeDelimited(buffer: string, enc: string): NodeRequestMessage;
	decodeHex(str: string): NodeRequestMessage;
	decodeJSON(str: string): NodeRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RangeInfo {

		

span?: PrettySpan;
		

getSpan?() : PrettySpan;
		setSpan?(span : PrettySpan): void;
		



raft_state?: string;
		

getRaftState?() : string;
		setRaftState?(raftState : string): void;
		



state?: storage.storagebase.RangeInfo;
		

getState?() : storage.storagebase.RangeInfo;
		setState?(state : storage.storagebase.RangeInfo): void;
		



}

	export interface RangeInfoMessage extends RangeInfo {
	add(key: string, value: any, noAssert?: boolean): RangeInfoMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RangeInfoMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RangeInfoBuilder {
	new(data?: RangeInfo): RangeInfoMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RangeInfoMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RangeInfoMessage;
	decode64(str: string) : RangeInfoMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: Buffer, enc: string): RangeInfoMessage;
	decodeDelimited(buffer: string, enc: string): RangeInfoMessage;
	decodeHex(str: string): RangeInfoMessage;
	decodeJSON(str: string): RangeInfoMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RangesRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface RangesRequestMessage extends RangesRequest {
	add(key: string, value: any, noAssert?: boolean): RangesRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RangesRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RangesRequestBuilder {
	new(data?: RangesRequest): RangesRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RangesRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RangesRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RangesRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RangesRequestMessage;
	decode64(str: string) : RangesRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RangesRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RangesRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): RangesRequestMessage;
	decodeDelimited(buffer: string, enc: string): RangesRequestMessage;
	decodeHex(str: string): RangesRequestMessage;
	decodeJSON(str: string): RangesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RangesResponse {

		

ranges?: RangeInfo[];
		

getRanges?() : RangeInfo[];
		setRanges?(ranges : RangeInfo[]): void;
		



}

	export interface RangesResponseMessage extends RangesResponse {
	add(key: string, value: any, noAssert?: boolean): RangesResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RangesResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RangesResponseBuilder {
	new(data?: RangesResponse): RangesResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RangesResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RangesResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RangesResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RangesResponseMessage;
	decode64(str: string) : RangesResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RangesResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RangesResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): RangesResponseMessage;
	decodeDelimited(buffer: string, enc: string): RangesResponseMessage;
	decodeHex(str: string): RangesResponseMessage;
	decodeJSON(str: string): RangesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GossipRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface GossipRequestMessage extends GossipRequest {
	add(key: string, value: any, noAssert?: boolean): GossipRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): GossipRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface GossipRequestBuilder {
	new(data?: GossipRequest): GossipRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : GossipRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : GossipRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : GossipRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : GossipRequestMessage;
	decode64(str: string) : GossipRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): GossipRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): GossipRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): GossipRequestMessage;
	decodeDelimited(buffer: string, enc: string): GossipRequestMessage;
	decodeHex(str: string): GossipRequestMessage;
	decodeJSON(str: string): GossipRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface JSONResponse {

		

data?: ByteBuffer;
		

getData?() : ByteBuffer;
		setData?(data : ByteBuffer): void;
		



}

	export interface JSONResponseMessage extends JSONResponse {
	add(key: string, value: any, noAssert?: boolean): JSONResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): JSONResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface JSONResponseBuilder {
	new(data?: JSONResponse): JSONResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : JSONResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : JSONResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : JSONResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : JSONResponseMessage;
	decode64(str: string) : JSONResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): JSONResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): JSONResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): JSONResponseMessage;
	decodeDelimited(buffer: string, enc: string): JSONResponseMessage;
	decodeHex(str: string): JSONResponseMessage;
	decodeJSON(str: string): JSONResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface LogsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



level?: string;
		

getLevel?() : string;
		setLevel?(level : string): void;
		



start_time?: string;
		

getStartTime?() : string;
		setStartTime?(startTime : string): void;
		



end_time?: string;
		

getEndTime?() : string;
		setEndTime?(endTime : string): void;
		



max?: string;
		

getMax?() : string;
		setMax?(max : string): void;
		



pattern?: string;
		

getPattern?() : string;
		setPattern?(pattern : string): void;
		



}

	export interface LogsRequestMessage extends LogsRequest {
	add(key: string, value: any, noAssert?: boolean): LogsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): LogsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface LogsRequestBuilder {
	new(data?: LogsRequest): LogsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : LogsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : LogsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : LogsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : LogsRequestMessage;
	decode64(str: string) : LogsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): LogsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): LogsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): LogsRequestMessage;
	decodeDelimited(buffer: string, enc: string): LogsRequestMessage;
	decodeHex(str: string): LogsRequestMessage;
	decodeJSON(str: string): LogsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface LogFilesListRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface LogFilesListRequestMessage extends LogFilesListRequest {
	add(key: string, value: any, noAssert?: boolean): LogFilesListRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): LogFilesListRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface LogFilesListRequestBuilder {
	new(data?: LogFilesListRequest): LogFilesListRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : LogFilesListRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : LogFilesListRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : LogFilesListRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : LogFilesListRequestMessage;
	decode64(str: string) : LogFilesListRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): LogFilesListRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): LogFilesListRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): LogFilesListRequestMessage;
	decodeDelimited(buffer: string, enc: string): LogFilesListRequestMessage;
	decodeHex(str: string): LogFilesListRequestMessage;
	decodeJSON(str: string): LogFilesListRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface LogFileRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



file?: string;
		

getFile?() : string;
		setFile?(file : string): void;
		



}

	export interface LogFileRequestMessage extends LogFileRequest {
	add(key: string, value: any, noAssert?: boolean): LogFileRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): LogFileRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface LogFileRequestBuilder {
	new(data?: LogFileRequest): LogFileRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : LogFileRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : LogFileRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : LogFileRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : LogFileRequestMessage;
	decode64(str: string) : LogFileRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): LogFileRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): LogFileRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): LogFileRequestMessage;
	decodeDelimited(buffer: string, enc: string): LogFileRequestMessage;
	decodeHex(str: string): LogFileRequestMessage;
	decodeJSON(str: string): LogFileRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface StacksRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface StacksRequestMessage extends StacksRequest {
	add(key: string, value: any, noAssert?: boolean): StacksRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StacksRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StacksRequestBuilder {
	new(data?: StacksRequest): StacksRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StacksRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StacksRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StacksRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StacksRequestMessage;
	decode64(str: string) : StacksRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StacksRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StacksRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): StacksRequestMessage;
	decodeDelimited(buffer: string, enc: string): StacksRequestMessage;
	decodeHex(str: string): StacksRequestMessage;
	decodeJSON(str: string): StacksRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface MetricsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface MetricsRequestMessage extends MetricsRequest {
	add(key: string, value: any, noAssert?: boolean): MetricsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): MetricsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface MetricsRequestBuilder {
	new(data?: MetricsRequest): MetricsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : MetricsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : MetricsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : MetricsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : MetricsRequestMessage;
	decode64(str: string) : MetricsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): MetricsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): MetricsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): MetricsRequestMessage;
	decodeDelimited(buffer: string, enc: string): MetricsRequestMessage;
	decodeHex(str: string): MetricsRequestMessage;
	decodeJSON(str: string): MetricsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftRangeNode {

		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



range?: RangeInfo;
		

getRange?() : RangeInfo;
		setRange?(range : RangeInfo): void;
		



}

	export interface RaftRangeNodeMessage extends RaftRangeNode {
	add(key: string, value: any, noAssert?: boolean): RaftRangeNodeMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftRangeNodeMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftRangeNodeBuilder {
	new(data?: RaftRangeNode): RaftRangeNodeMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftRangeNodeMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftRangeNodeMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftRangeNodeMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftRangeNodeMessage;
	decode64(str: string) : RaftRangeNodeMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftRangeNodeMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftRangeNodeMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftRangeNodeMessage;
	decodeDelimited(buffer: string, enc: string): RaftRangeNodeMessage;
	decodeHex(str: string): RaftRangeNodeMessage;
	decodeJSON(str: string): RaftRangeNodeMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftRangeError {

		

message?: string;
		

getMessage?() : string;
		setMessage?(message : string): void;
		



}

	export interface RaftRangeErrorMessage extends RaftRangeError {
	add(key: string, value: any, noAssert?: boolean): RaftRangeErrorMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftRangeErrorMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftRangeErrorBuilder {
	new(data?: RaftRangeError): RaftRangeErrorMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftRangeErrorMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftRangeErrorMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftRangeErrorMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftRangeErrorMessage;
	decode64(str: string) : RaftRangeErrorMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftRangeErrorMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftRangeErrorMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftRangeErrorMessage;
	decodeDelimited(buffer: string, enc: string): RaftRangeErrorMessage;
	decodeHex(str: string): RaftRangeErrorMessage;
	decodeJSON(str: string): RaftRangeErrorMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftRangeStatus {

		

range_id?: Long;
		

getRangeId?() : Long;
		setRangeId?(rangeId : Long): void;
		



errors?: RaftRangeError[];
		

getErrors?() : RaftRangeError[];
		setErrors?(errors : RaftRangeError[]): void;
		



nodes?: RaftRangeNode[];
		

getNodes?() : RaftRangeNode[];
		setNodes?(nodes : RaftRangeNode[]): void;
		



}

	export interface RaftRangeStatusMessage extends RaftRangeStatus {
	add(key: string, value: any, noAssert?: boolean): RaftRangeStatusMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftRangeStatusMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftRangeStatusBuilder {
	new(data?: RaftRangeStatus): RaftRangeStatusMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftRangeStatusMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftRangeStatusMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftRangeStatusMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftRangeStatusMessage;
	decode64(str: string) : RaftRangeStatusMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftRangeStatusMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftRangeStatusMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftRangeStatusMessage;
	decodeDelimited(buffer: string, enc: string): RaftRangeStatusMessage;
	decodeHex(str: string): RaftRangeStatusMessage;
	decodeJSON(str: string): RaftRangeStatusMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftDebugRequest {

		

}

	export interface RaftDebugRequestMessage extends RaftDebugRequest {
	add(key: string, value: any, noAssert?: boolean): RaftDebugRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftDebugRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftDebugRequestBuilder {
	new(data?: RaftDebugRequest): RaftDebugRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftDebugRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftDebugRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftDebugRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftDebugRequestMessage;
	decode64(str: string) : RaftDebugRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftDebugRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftDebugRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftDebugRequestMessage;
	decodeDelimited(buffer: string, enc: string): RaftDebugRequestMessage;
	decodeHex(str: string): RaftDebugRequestMessage;
	decodeJSON(str: string): RaftDebugRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftDebugResponse {

		

ranges?: ProtoBufMap<Long, RaftRangeStatus>;
		

getRanges?() : ProtoBufMap<Long, RaftRangeStatus>;
		setRanges?(ranges : ProtoBufMap<Long, RaftRangeStatus>): void;
		



}

	export interface RaftDebugResponseMessage extends RaftDebugResponse {
	add(key: string, value: any, noAssert?: boolean): RaftDebugResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RaftDebugResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RaftDebugResponseBuilder {
	new(data?: RaftDebugResponse): RaftDebugResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RaftDebugResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RaftDebugResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RaftDebugResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RaftDebugResponseMessage;
	decode64(str: string) : RaftDebugResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RaftDebugResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RaftDebugResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): RaftDebugResponseMessage;
	decodeDelimited(buffer: string, enc: string): RaftDebugResponseMessage;
	decodeHex(str: string): RaftDebugResponseMessage;
	decodeJSON(str: string): RaftDebugResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface SpanStatsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



start_key?: ByteBuffer;
		

getStartKey?() : ByteBuffer;
		setStartKey?(startKey : ByteBuffer): void;
		



end_key?: ByteBuffer;
		

getEndKey?() : ByteBuffer;
		setEndKey?(endKey : ByteBuffer): void;
		



}

	export interface SpanStatsRequestMessage extends SpanStatsRequest {
	add(key: string, value: any, noAssert?: boolean): SpanStatsRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SpanStatsRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SpanStatsRequestBuilder {
	new(data?: SpanStatsRequest): SpanStatsRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SpanStatsRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SpanStatsRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SpanStatsRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SpanStatsRequestMessage;
	decode64(str: string) : SpanStatsRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SpanStatsRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SpanStatsRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): SpanStatsRequestMessage;
	decodeDelimited(buffer: string, enc: string): SpanStatsRequestMessage;
	decodeHex(str: string): SpanStatsRequestMessage;
	decodeJSON(str: string): SpanStatsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface SpanStatsResponse {

		

range_count?: number;
		

getRangeCount?() : number;
		setRangeCount?(rangeCount : number): void;
		



total_stats?: storage.engine.enginepb.MVCCStats;
		

getTotalStats?() : storage.engine.enginepb.MVCCStats;
		setTotalStats?(totalStats : storage.engine.enginepb.MVCCStats): void;
		



}

	export interface SpanStatsResponseMessage extends SpanStatsResponse {
	add(key: string, value: any, noAssert?: boolean): SpanStatsResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): SpanStatsResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface SpanStatsResponseBuilder {
	new(data?: SpanStatsResponse): SpanStatsResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : SpanStatsResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : SpanStatsResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : SpanStatsResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : SpanStatsResponseMessage;
	decode64(str: string) : SpanStatsResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): SpanStatsResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): SpanStatsResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): SpanStatsResponseMessage;
	decodeDelimited(buffer: string, enc: string): SpanStatsResponseMessage;
	decodeHex(str: string): SpanStatsResponseMessage;
	decodeJSON(str: string): SpanStatsResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface PrettySpan {

		

start_key?: string;
		

getStartKey?() : string;
		setStartKey?(startKey : string): void;
		



end_key?: string;
		

getEndKey?() : string;
		setEndKey?(endKey : string): void;
		



}

	export interface PrettySpanMessage extends PrettySpan {
	add(key: string, value: any, noAssert?: boolean): PrettySpanMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): PrettySpanMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface PrettySpanBuilder {
	new(data?: PrettySpan): PrettySpanMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : PrettySpanMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : PrettySpanMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : PrettySpanMessage;
	decode(buffer: string, length?: number | string, enc?: string) : PrettySpanMessage;
	decode64(str: string) : PrettySpanMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): PrettySpanMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): PrettySpanMessage;
	decodeDelimited(buffer: Buffer, enc: string): PrettySpanMessage;
	decodeDelimited(buffer: string, enc: string): PrettySpanMessage;
	decodeHex(str: string): PrettySpanMessage;
	decodeJSON(str: string): PrettySpanMessage;
	
}

}


declare module cockroach.server.serverpb {
	export const enum ZoneConfigurationLevel {
		UNKNOWN = 0,
		CLUSTER = 1,
		DATABASE = 2,
		TABLE = 3,
		
}
}

declare module cockroach.server.serverpb {
	export const enum DrainMode {
		CLIENT = 0,
		LEASES = 1,
		
}
}


declare module cockroach.server {

	export interface status {

		

}

	export interface statusMessage extends status {
	add(key: string, value: any, noAssert?: boolean): statusMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): statusMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface statusBuilder {
	new(data?: status): statusMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : statusMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : statusMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : statusMessage;
	decode(buffer: string, length?: number | string, enc?: string) : statusMessage;
	decode64(str: string) : statusMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): statusMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): statusMessage;
	decodeDelimited(buffer: Buffer, enc: string): statusMessage;
	decodeDelimited(buffer: string, enc: string): statusMessage;
	decodeHex(str: string): statusMessage;
	decodeJSON(str: string): statusMessage;
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
	add(key: string, value: any, noAssert?: boolean): StoreStatusMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): StoreStatusMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface StoreStatusBuilder {
	new(data?: StoreStatus): StoreStatusMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : StoreStatusMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : StoreStatusMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : StoreStatusMessage;
	decode(buffer: string, length?: number | string, enc?: string) : StoreStatusMessage;
	decode64(str: string) : StoreStatusMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): StoreStatusMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): StoreStatusMessage;
	decodeDelimited(buffer: Buffer, enc: string): StoreStatusMessage;
	decodeDelimited(buffer: string, enc: string): StoreStatusMessage;
	decodeHex(str: string): StoreStatusMessage;
	decodeJSON(str: string): StoreStatusMessage;
	
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
	add(key: string, value: any, noAssert?: boolean): NodeStatusMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): NodeStatusMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface NodeStatusBuilder {
	new(data?: NodeStatus): NodeStatusMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : NodeStatusMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : NodeStatusMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : NodeStatusMessage;
	decode(buffer: string, length?: number | string, enc?: string) : NodeStatusMessage;
	decode64(str: string) : NodeStatusMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): NodeStatusMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): NodeStatusMessage;
	decodeDelimited(buffer: Buffer, enc: string): NodeStatusMessage;
	decodeDelimited(buffer: string, enc: string): NodeStatusMessage;
	decodeHex(str: string): NodeStatusMessage;
	decodeJSON(str: string): NodeStatusMessage;
	
}

}




declare module cockroach {

	export interface build {

		

}

	export interface buildMessage extends build {
	add(key: string, value: any, noAssert?: boolean): buildMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): buildMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface buildBuilder {
	new(data?: build): buildMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : buildMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : buildMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : buildMessage;
	decode(buffer: string, length?: number | string, enc?: string) : buildMessage;
	decode64(str: string) : buildMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): buildMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): buildMessage;
	decodeDelimited(buffer: Buffer, enc: string): buildMessage;
	decodeDelimited(buffer: string, enc: string): buildMessage;
	decodeHex(str: string): buildMessage;
	decodeJSON(str: string): buildMessage;
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
	add(key: string, value: any, noAssert?: boolean): InfoMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): InfoMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface InfoBuilder {
	new(data?: Info): InfoMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: string, length?: number | string, enc?: string) : InfoMessage;
	decode64(str: string) : InfoMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): InfoMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): InfoMessage;
	decodeDelimited(buffer: Buffer, enc: string): InfoMessage;
	decodeDelimited(buffer: string, enc: string): InfoMessage;
	decodeHex(str: string): InfoMessage;
	decodeJSON(str: string): InfoMessage;
	
}

}



declare module cockroach {

	export interface gossip {

		

}

	export interface gossipMessage extends gossip {
	add(key: string, value: any, noAssert?: boolean): gossipMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): gossipMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface gossipBuilder {
	new(data?: gossip): gossipMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : gossipMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : gossipMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : gossipMessage;
	decode(buffer: string, length?: number | string, enc?: string) : gossipMessage;
	decode64(str: string) : gossipMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): gossipMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): gossipMessage;
	decodeDelimited(buffer: Buffer, enc: string): gossipMessage;
	decodeDelimited(buffer: string, enc: string): gossipMessage;
	decodeHex(str: string): gossipMessage;
	decodeJSON(str: string): gossipMessage;
	BootstrapInfo: gossip.BootstrapInfoBuilder;
	Request: gossip.RequestBuilder;
	Response: gossip.ResponseBuilder;
	InfoStatus: gossip.InfoStatusBuilder;
	Info: gossip.InfoBuilder;
	
}

}

declare module cockroach.gossip {

	export interface BootstrapInfo {

		

addresses?: util.UnresolvedAddr[];
		

getAddresses?() : util.UnresolvedAddr[];
		setAddresses?(addresses : util.UnresolvedAddr[]): void;
		



timestamp?: util.hlc.Timestamp;
		

getTimestamp?() : util.hlc.Timestamp;
		setTimestamp?(timestamp : util.hlc.Timestamp): void;
		



}

	export interface BootstrapInfoMessage extends BootstrapInfo {
	add(key: string, value: any, noAssert?: boolean): BootstrapInfoMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): BootstrapInfoMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface BootstrapInfoBuilder {
	new(data?: BootstrapInfo): BootstrapInfoMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : BootstrapInfoMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : BootstrapInfoMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : BootstrapInfoMessage;
	decode(buffer: string, length?: number | string, enc?: string) : BootstrapInfoMessage;
	decode64(str: string) : BootstrapInfoMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): BootstrapInfoMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): BootstrapInfoMessage;
	decodeDelimited(buffer: Buffer, enc: string): BootstrapInfoMessage;
	decodeDelimited(buffer: string, enc: string): BootstrapInfoMessage;
	decodeHex(str: string): BootstrapInfoMessage;
	decodeJSON(str: string): BootstrapInfoMessage;
	
}

}


declare module cockroach.gossip {

	export interface Request {

		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



addr?: util.UnresolvedAddr;
		

getAddr?() : util.UnresolvedAddr;
		setAddr?(addr : util.UnresolvedAddr): void;
		



high_water_stamps?: ProtoBufMap<number, Long>;
		

getHighWaterStamps?() : ProtoBufMap<number, Long>;
		setHighWaterStamps?(highWaterStamps : ProtoBufMap<number, Long>): void;
		



delta?: ProtoBufMap<string, Info>;
		

getDelta?() : ProtoBufMap<string, Info>;
		setDelta?(delta : ProtoBufMap<string, Info>): void;
		



}

	export interface RequestMessage extends Request {
	add(key: string, value: any, noAssert?: boolean): RequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): RequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface RequestBuilder {
	new(data?: Request): RequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : RequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : RequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : RequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : RequestMessage;
	decode64(str: string) : RequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): RequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): RequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): RequestMessage;
	decodeDelimited(buffer: string, enc: string): RequestMessage;
	decodeHex(str: string): RequestMessage;
	decodeJSON(str: string): RequestMessage;
	
}

}


declare module cockroach.gossip {

	export interface Response {

		

node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



addr?: util.UnresolvedAddr;
		

getAddr?() : util.UnresolvedAddr;
		setAddr?(addr : util.UnresolvedAddr): void;
		



alternate_addr?: util.UnresolvedAddr;
		

getAlternateAddr?() : util.UnresolvedAddr;
		setAlternateAddr?(alternateAddr : util.UnresolvedAddr): void;
		



alternate_node_id?: number;
		

getAlternateNodeId?() : number;
		setAlternateNodeId?(alternateNodeId : number): void;
		



delta?: ProtoBufMap<string, Info>;
		

getDelta?() : ProtoBufMap<string, Info>;
		setDelta?(delta : ProtoBufMap<string, Info>): void;
		



high_water_stamps?: ProtoBufMap<number, Long>;
		

getHighWaterStamps?() : ProtoBufMap<number, Long>;
		setHighWaterStamps?(highWaterStamps : ProtoBufMap<number, Long>): void;
		



}

	export interface ResponseMessage extends Response {
	add(key: string, value: any, noAssert?: boolean): ResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ResponseBuilder {
	new(data?: Response): ResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ResponseMessage;
	decode64(str: string) : ResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): ResponseMessage;
	decodeDelimited(buffer: string, enc: string): ResponseMessage;
	decodeHex(str: string): ResponseMessage;
	decodeJSON(str: string): ResponseMessage;
	
}

}


declare module cockroach.gossip {

	export interface InfoStatus {

		

infos?: ProtoBufMap<string, Info>;
		

getInfos?() : ProtoBufMap<string, Info>;
		setInfos?(infos : ProtoBufMap<string, Info>): void;
		



}

	export interface InfoStatusMessage extends InfoStatus {
	add(key: string, value: any, noAssert?: boolean): InfoStatusMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): InfoStatusMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface InfoStatusBuilder {
	new(data?: InfoStatus): InfoStatusMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : InfoStatusMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : InfoStatusMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : InfoStatusMessage;
	decode(buffer: string, length?: number | string, enc?: string) : InfoStatusMessage;
	decode64(str: string) : InfoStatusMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): InfoStatusMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): InfoStatusMessage;
	decodeDelimited(buffer: Buffer, enc: string): InfoStatusMessage;
	decodeDelimited(buffer: string, enc: string): InfoStatusMessage;
	decodeHex(str: string): InfoStatusMessage;
	decodeJSON(str: string): InfoStatusMessage;
	
}

}


declare module cockroach.gossip {

	export interface Info {

		

value?: roachpb.Value;
		

getValue?() : roachpb.Value;
		setValue?(value : roachpb.Value): void;
		



orig_stamp?: Long;
		

getOrigStamp?() : Long;
		setOrigStamp?(origStamp : Long): void;
		



ttl_stamp?: Long;
		

getTtlStamp?() : Long;
		setTtlStamp?(ttlStamp : Long): void;
		



hops?: number;
		

getHops?() : number;
		setHops?(hops : number): void;
		



node_id?: number;
		

getNodeId?() : number;
		setNodeId?(nodeId : number): void;
		



peer_id?: number;
		

getPeerId?() : number;
		setPeerId?(peerId : number): void;
		



}

	export interface InfoMessage extends Info {
	add(key: string, value: any, noAssert?: boolean): InfoMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): InfoMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface InfoBuilder {
	new(data?: Info): InfoMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : InfoMessage;
	decode(buffer: string, length?: number | string, enc?: string) : InfoMessage;
	decode64(str: string) : InfoMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): InfoMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): InfoMessage;
	decodeDelimited(buffer: Buffer, enc: string): InfoMessage;
	decodeDelimited(buffer: string, enc: string): InfoMessage;
	decodeHex(str: string): InfoMessage;
	decodeJSON(str: string): InfoMessage;
	
}

}



declare module cockroach {

	export interface ts {

		

}

	export interface tsMessage extends ts {
	add(key: string, value: any, noAssert?: boolean): tsMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): tsMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface tsBuilder {
	new(data?: ts): tsMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : tsMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : tsMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : tsMessage;
	decode(buffer: string, length?: number | string, enc?: string) : tsMessage;
	decode64(str: string) : tsMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): tsMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): tsMessage;
	decodeDelimited(buffer: Buffer, enc: string): tsMessage;
	decodeDelimited(buffer: string, enc: string): tsMessage;
	decodeHex(str: string): tsMessage;
	decodeJSON(str: string): tsMessage;
	tspb: ts.tspbBuilder;
	
}

}

declare module cockroach.ts {

	export interface tspb {

		

}

	export interface tspbMessage extends tspb {
	add(key: string, value: any, noAssert?: boolean): tspbMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): tspbMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface tspbBuilder {
	new(data?: tspb): tspbMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : tspbMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : tspbMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : tspbMessage;
	decode(buffer: string, length?: number | string, enc?: string) : tspbMessage;
	decode64(str: string) : tspbMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): tspbMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): tspbMessage;
	decodeDelimited(buffer: Buffer, enc: string): tspbMessage;
	decodeDelimited(buffer: string, enc: string): tspbMessage;
	decodeHex(str: string): tspbMessage;
	decodeJSON(str: string): tspbMessage;
	TimeSeriesDatapoint: tspb.TimeSeriesDatapointBuilder;
	TimeSeriesData: tspb.TimeSeriesDataBuilder;
	Query: tspb.QueryBuilder;
	TimeSeriesQueryRequest: tspb.TimeSeriesQueryRequestBuilder;
	TimeSeriesQueryResponse: tspb.TimeSeriesQueryResponseBuilder;
	TimeSeriesQueryAggregator: tspb.TimeSeriesQueryAggregator;
	TimeSeriesQueryDerivative: tspb.TimeSeriesQueryDerivative;
	
}

}

declare module cockroach.ts.tspb {

	export interface TimeSeriesDatapoint {

		

timestamp_nanos?: Long;
		

getTimestampNanos?() : Long;
		setTimestampNanos?(timestampNanos : Long): void;
		



value?: number;
		

getValue?() : number;
		setValue?(value : number): void;
		



}

	export interface TimeSeriesDatapointMessage extends TimeSeriesDatapoint {
	add(key: string, value: any, noAssert?: boolean): TimeSeriesDatapointMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimeSeriesDatapointMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimeSeriesDatapointBuilder {
	new(data?: TimeSeriesDatapoint): TimeSeriesDatapointMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimeSeriesDatapointMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimeSeriesDatapointMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimeSeriesDatapointMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimeSeriesDatapointMessage;
	decode64(str: string) : TimeSeriesDatapointMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimeSeriesDatapointMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimeSeriesDatapointMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimeSeriesDatapointMessage;
	decodeDelimited(buffer: string, enc: string): TimeSeriesDatapointMessage;
	decodeHex(str: string): TimeSeriesDatapointMessage;
	decodeJSON(str: string): TimeSeriesDatapointMessage;
	
}

}


declare module cockroach.ts.tspb {

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
	add(key: string, value: any, noAssert?: boolean): TimeSeriesDataMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimeSeriesDataMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimeSeriesDataBuilder {
	new(data?: TimeSeriesData): TimeSeriesDataMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimeSeriesDataMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimeSeriesDataMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimeSeriesDataMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimeSeriesDataMessage;
	decode64(str: string) : TimeSeriesDataMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimeSeriesDataMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimeSeriesDataMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimeSeriesDataMessage;
	decodeDelimited(buffer: string, enc: string): TimeSeriesDataMessage;
	decodeHex(str: string): TimeSeriesDataMessage;
	decodeJSON(str: string): TimeSeriesDataMessage;
	
}

}


declare module cockroach.ts.tspb {

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
	add(key: string, value: any, noAssert?: boolean): QueryMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): QueryMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface QueryBuilder {
	new(data?: Query): QueryMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : QueryMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : QueryMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : QueryMessage;
	decode(buffer: string, length?: number | string, enc?: string) : QueryMessage;
	decode64(str: string) : QueryMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): QueryMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): QueryMessage;
	decodeDelimited(buffer: Buffer, enc: string): QueryMessage;
	decodeDelimited(buffer: string, enc: string): QueryMessage;
	decodeHex(str: string): QueryMessage;
	decodeJSON(str: string): QueryMessage;
	
}

}


declare module cockroach.ts.tspb {

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
	add(key: string, value: any, noAssert?: boolean): TimeSeriesQueryRequestMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimeSeriesQueryRequestMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimeSeriesQueryRequestBuilder {
	new(data?: TimeSeriesQueryRequest): TimeSeriesQueryRequestMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimeSeriesQueryRequestMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimeSeriesQueryRequestMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimeSeriesQueryRequestMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimeSeriesQueryRequestMessage;
	decode64(str: string) : TimeSeriesQueryRequestMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimeSeriesQueryRequestMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimeSeriesQueryRequestMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimeSeriesQueryRequestMessage;
	decodeDelimited(buffer: string, enc: string): TimeSeriesQueryRequestMessage;
	decodeHex(str: string): TimeSeriesQueryRequestMessage;
	decodeJSON(str: string): TimeSeriesQueryRequestMessage;
	
}

}


declare module cockroach.ts.tspb {

	export interface TimeSeriesQueryResponse {

		

results?: TimeSeriesQueryResponse.Result[];
		

getResults?() : TimeSeriesQueryResponse.Result[];
		setResults?(results : TimeSeriesQueryResponse.Result[]): void;
		



}

	export interface TimeSeriesQueryResponseMessage extends TimeSeriesQueryResponse {
	add(key: string, value: any, noAssert?: boolean): TimeSeriesQueryResponseMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): TimeSeriesQueryResponseMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface TimeSeriesQueryResponseBuilder {
	new(data?: TimeSeriesQueryResponse): TimeSeriesQueryResponseMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : TimeSeriesQueryResponseMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : TimeSeriesQueryResponseMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : TimeSeriesQueryResponseMessage;
	decode(buffer: string, length?: number | string, enc?: string) : TimeSeriesQueryResponseMessage;
	decode64(str: string) : TimeSeriesQueryResponseMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): TimeSeriesQueryResponseMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): TimeSeriesQueryResponseMessage;
	decodeDelimited(buffer: Buffer, enc: string): TimeSeriesQueryResponseMessage;
	decodeDelimited(buffer: string, enc: string): TimeSeriesQueryResponseMessage;
	decodeHex(str: string): TimeSeriesQueryResponseMessage;
	decodeJSON(str: string): TimeSeriesQueryResponseMessage;
	Result: TimeSeriesQueryResponse.ResultBuilder;
	
}

}

declare module cockroach.ts.tspb.TimeSeriesQueryResponse {

	export interface Result {

		

query?: Query;
		

getQuery?() : Query;
		setQuery?(query : Query): void;
		



datapoints?: TimeSeriesDatapoint[];
		

getDatapoints?() : TimeSeriesDatapoint[];
		setDatapoints?(datapoints : TimeSeriesDatapoint[]): void;
		



}

	export interface ResultMessage extends Result {
	add(key: string, value: any, noAssert?: boolean): ResultMessage;
	calculate(): number;
	encode64(): string;
	encodeAB(): ArrayBuffer;
	encodeDelimited(buffer?: ByteBuffer, noVerify?: boolean): ByteBuffer;
	encodeDelimited(buffer?: boolean, noVerify?: boolean): ByteBuffer;
	encodeHex(): string;
	encodeJSON(): string;
	encodeNB(): Buffer;
	get(key: string, noAssert: boolean): any;
	set(keyOrObj: string, value: any | boolean, noAssert: boolean): ResultMessage;
	toArrayBuffer(): ArrayBuffer;
	toBase64(): string;
	toBuffer(): Buffer;
	toHex(): string;
	toRaw(): any;
	toString(): string;
}

export interface ResultBuilder {
	new(data?: Result): ResultMessage;
	decode(buffer: ArrayBuffer, length?: number | string, enc?: string) : ResultMessage;
	decode(buffer: ByteBuffer, length?: number | string, enc?: string) : ResultMessage;
	decode(buffer: Buffer, length?: number | string, enc?: string) : ResultMessage;
	decode(buffer: string, length?: number | string, enc?: string) : ResultMessage;
	decode64(str: string) : ResultMessage;
	decodeDelimited(buffer: ArrayBuffer, enc: string): ResultMessage;
	decodeDelimited(buffer: ByteBuffer, enc: string): ResultMessage;
	decodeDelimited(buffer: Buffer, enc: string): ResultMessage;
	decodeDelimited(buffer: string, enc: string): ResultMessage;
	decodeHex(str: string): ResultMessage;
	decodeJSON(str: string): ResultMessage;
	
}

}



declare module cockroach.ts.tspb {
	export const enum TimeSeriesQueryAggregator {
		AVG = 1,
		SUM = 2,
		MAX = 3,
		MIN = 4,
		
}
}

declare module cockroach.ts.tspb {
	export const enum TimeSeriesQueryDerivative {
		NONE = 0,
		DERIVATIVE = 1,
		NON_NEGATIVE_DERIVATIVE = 2,
		
}
}




