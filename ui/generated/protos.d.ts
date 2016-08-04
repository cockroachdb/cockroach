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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface utilBuilder {
	new(data?: util): utilMessage;
	decode(buffer: ArrayBuffer) : utilMessage;
	decode(buffer: ByteBuffer) : utilMessage;
	decode64(buffer: string) : utilMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface UnresolvedAddrBuilder {
	new(data?: UnresolvedAddr): UnresolvedAddrMessage;
	decode(buffer: ArrayBuffer) : UnresolvedAddrMessage;
	decode(buffer: ByteBuffer) : UnresolvedAddrMessage;
	decode64(buffer: string) : UnresolvedAddrMessage;
	
}

}


declare module cockroach.util {

	export interface hlc {

		

}

	export interface hlcMessage extends hlc {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface hlcBuilder {
	new(data?: hlc): hlcMessage;
	decode(buffer: ArrayBuffer) : hlcMessage;
	decode(buffer: ByteBuffer) : hlcMessage;
	decode64(buffer: string) : hlcMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer) : TimestampMessage;
	decode(buffer: ByteBuffer) : TimestampMessage;
	decode64(buffer: string) : TimestampMessage;
	
}

}




declare module cockroach {

	export interface roachpb {

		

}

	export interface roachpbMessage extends roachpb {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface roachpbBuilder {
	new(data?: roachpb): roachpbMessage;
	decode(buffer: ArrayBuffer) : roachpbMessage;
	decode(buffer: ByteBuffer) : roachpbMessage;
	decode64(buffer: string) : roachpbMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface AttributesBuilder {
	new(data?: Attributes): AttributesMessage;
	decode(buffer: ArrayBuffer) : AttributesMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ReplicaDescriptorBuilder {
	new(data?: ReplicaDescriptor): ReplicaDescriptorMessage;
	decode(buffer: ArrayBuffer) : ReplicaDescriptorMessage;
	decode(buffer: ByteBuffer) : ReplicaDescriptorMessage;
	decode64(buffer: string) : ReplicaDescriptorMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ReplicaIdentBuilder {
	new(data?: ReplicaIdent): ReplicaIdentMessage;
	decode(buffer: ArrayBuffer) : ReplicaIdentMessage;
	decode(buffer: ByteBuffer) : ReplicaIdentMessage;
	decode64(buffer: string) : ReplicaIdentMessage;
	
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RangeDescriptorBuilder {
	new(data?: RangeDescriptor): RangeDescriptorMessage;
	decode(buffer: ArrayBuffer) : RangeDescriptorMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StoreCapacityBuilder {
	new(data?: StoreCapacity): StoreCapacityMessage;
	decode(buffer: ArrayBuffer) : StoreCapacityMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface NodeDescriptorBuilder {
	new(data?: NodeDescriptor): NodeDescriptorMessage;
	decode(buffer: ArrayBuffer) : NodeDescriptorMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StoreDescriptorBuilder {
	new(data?: StoreDescriptor): StoreDescriptorMessage;
	decode(buffer: ArrayBuffer) : StoreDescriptorMessage;
	decode(buffer: ByteBuffer) : StoreDescriptorMessage;
	decode64(buffer: string) : StoreDescriptorMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StoreDeadReplicasBuilder {
	new(data?: StoreDeadReplicas): StoreDeadReplicasMessage;
	decode(buffer: ArrayBuffer) : StoreDeadReplicasMessage;
	decode(buffer: ByteBuffer) : StoreDeadReplicasMessage;
	decode64(buffer: string) : StoreDeadReplicasMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SpanBuilder {
	new(data?: Span): SpanMessage;
	decode(buffer: ArrayBuffer) : SpanMessage;
	decode(buffer: ByteBuffer) : SpanMessage;
	decode64(buffer: string) : SpanMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ValueBuilder {
	new(data?: Value): ValueMessage;
	decode(buffer: ArrayBuffer) : ValueMessage;
	decode(buffer: ByteBuffer) : ValueMessage;
	decode64(buffer: string) : ValueMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface KeyValueBuilder {
	new(data?: KeyValue): KeyValueMessage;
	decode(buffer: ArrayBuffer) : KeyValueMessage;
	decode(buffer: ByteBuffer) : KeyValueMessage;
	decode64(buffer: string) : KeyValueMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StoreIdentBuilder {
	new(data?: StoreIdent): StoreIdentMessage;
	decode(buffer: ArrayBuffer) : StoreIdentMessage;
	decode(buffer: ByteBuffer) : StoreIdentMessage;
	decode64(buffer: string) : StoreIdentMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SplitTriggerBuilder {
	new(data?: SplitTrigger): SplitTriggerMessage;
	decode(buffer: ArrayBuffer) : SplitTriggerMessage;
	decode(buffer: ByteBuffer) : SplitTriggerMessage;
	decode64(buffer: string) : SplitTriggerMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface MergeTriggerBuilder {
	new(data?: MergeTrigger): MergeTriggerMessage;
	decode(buffer: ArrayBuffer) : MergeTriggerMessage;
	decode(buffer: ByteBuffer) : MergeTriggerMessage;
	decode64(buffer: string) : MergeTriggerMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ChangeReplicasTriggerBuilder {
	new(data?: ChangeReplicasTrigger): ChangeReplicasTriggerMessage;
	decode(buffer: ArrayBuffer) : ChangeReplicasTriggerMessage;
	decode(buffer: ByteBuffer) : ChangeReplicasTriggerMessage;
	decode64(buffer: string) : ChangeReplicasTriggerMessage;
	
}

}


declare module cockroach.roachpb {

	export interface ModifiedSpanTrigger {

		

system_config_span?: boolean;
		

getSystemConfigSpan?() : boolean;
		setSystemConfigSpan?(systemConfigSpan : boolean): void;
		



}

	export interface ModifiedSpanTriggerMessage extends ModifiedSpanTrigger {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ModifiedSpanTriggerBuilder {
	new(data?: ModifiedSpanTrigger): ModifiedSpanTriggerMessage;
	decode(buffer: ArrayBuffer) : ModifiedSpanTriggerMessage;
	decode(buffer: ByteBuffer) : ModifiedSpanTriggerMessage;
	decode64(buffer: string) : ModifiedSpanTriggerMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface InternalCommitTriggerBuilder {
	new(data?: InternalCommitTrigger): InternalCommitTriggerMessage;
	decode(buffer: ArrayBuffer) : InternalCommitTriggerMessage;
	decode(buffer: ByteBuffer) : InternalCommitTriggerMessage;
	decode64(buffer: string) : InternalCommitTriggerMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TransactionBuilder {
	new(data?: Transaction): TransactionMessage;
	decode(buffer: ArrayBuffer) : TransactionMessage;
	decode(buffer: ByteBuffer) : TransactionMessage;
	decode64(buffer: string) : TransactionMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface IntentBuilder {
	new(data?: Intent): IntentMessage;
	decode(buffer: ArrayBuffer) : IntentMessage;
	decode(buffer: ByteBuffer) : IntentMessage;
	decode64(buffer: string) : IntentMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface LeaseBuilder {
	new(data?: Lease): LeaseMessage;
	decode(buffer: ArrayBuffer) : LeaseMessage;
	decode(buffer: ByteBuffer) : LeaseMessage;
	decode64(buffer: string) : LeaseMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface AbortCacheEntryBuilder {
	new(data?: AbortCacheEntry): AbortCacheEntryMessage;
	decode(buffer: ArrayBuffer) : AbortCacheEntryMessage;
	decode(buffer: ByteBuffer) : AbortCacheEntryMessage;
	decode64(buffer: string) : AbortCacheEntryMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftTruncatedStateBuilder {
	new(data?: RaftTruncatedState): RaftTruncatedStateMessage;
	decode(buffer: ArrayBuffer) : RaftTruncatedStateMessage;
	decode(buffer: ByteBuffer) : RaftTruncatedStateMessage;
	decode64(buffer: string) : RaftTruncatedStateMessage;
	
}

}


declare module cockroach.roachpb {

	export interface RaftTombstone {

		

next_replica_id?: number;
		

getNextReplicaId?() : number;
		setNextReplicaId?(nextReplicaId : number): void;
		



}

	export interface RaftTombstoneMessage extends RaftTombstone {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftTombstoneBuilder {
	new(data?: RaftTombstone): RaftTombstoneMessage;
	decode(buffer: ArrayBuffer) : RaftTombstoneMessage;
	decode(buffer: ByteBuffer) : RaftTombstoneMessage;
	decode64(buffer: string) : RaftTombstoneMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftSnapshotDataBuilder {
	new(data?: RaftSnapshotData): RaftSnapshotDataMessage;
	decode(buffer: ArrayBuffer) : RaftSnapshotDataMessage;
	decode(buffer: ByteBuffer) : RaftSnapshotDataMessage;
	decode64(buffer: string) : RaftSnapshotDataMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface KeyValueBuilder {
	new(data?: KeyValue): KeyValueMessage;
	decode(buffer: ArrayBuffer) : KeyValueMessage;
	decode(buffer: ByteBuffer) : KeyValueMessage;
	decode64(buffer: string) : KeyValueMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface storageBuilder {
	new(data?: storage): storageMessage;
	decode(buffer: ArrayBuffer) : storageMessage;
	decode(buffer: ByteBuffer) : storageMessage;
	decode64(buffer: string) : storageMessage;
	engine: storage.engineBuilder;
	storagebase: storage.storagebaseBuilder;
	
}

}

declare module cockroach.storage {

	export interface engine {

		

}

	export interface engineMessage extends engine {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface engineBuilder {
	new(data?: engine): engineMessage;
	decode(buffer: ArrayBuffer) : engineMessage;
	decode(buffer: ByteBuffer) : engineMessage;
	decode64(buffer: string) : engineMessage;
	enginepb: engine.enginepbBuilder;
	
}

}

declare module cockroach.storage.engine {

	export interface enginepb {

		

}

	export interface enginepbMessage extends enginepb {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface enginepbBuilder {
	new(data?: enginepb): enginepbMessage;
	decode(buffer: ArrayBuffer) : enginepbMessage;
	decode(buffer: ByteBuffer) : enginepbMessage;
	decode64(buffer: string) : enginepbMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TxnMetaBuilder {
	new(data?: TxnMeta): TxnMetaMessage;
	decode(buffer: ArrayBuffer) : TxnMetaMessage;
	decode(buffer: ByteBuffer) : TxnMetaMessage;
	decode64(buffer: string) : TxnMetaMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface MVCCMetadataBuilder {
	new(data?: MVCCMetadata): MVCCMetadataMessage;
	decode(buffer: ArrayBuffer) : MVCCMetadataMessage;
	decode(buffer: ByteBuffer) : MVCCMetadataMessage;
	decode64(buffer: string) : MVCCMetadataMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface MVCCStatsBuilder {
	new(data?: MVCCStats): MVCCStatsMessage;
	decode(buffer: ArrayBuffer) : MVCCStatsMessage;
	decode(buffer: ByteBuffer) : MVCCStatsMessage;
	decode64(buffer: string) : MVCCStatsMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface storagebaseBuilder {
	new(data?: storagebase): storagebaseMessage;
	decode(buffer: ArrayBuffer) : storagebaseMessage;
	decode(buffer: ByteBuffer) : storagebaseMessage;
	decode64(buffer: string) : storagebaseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ReplicaStateBuilder {
	new(data?: ReplicaState): ReplicaStateMessage;
	decode(buffer: ArrayBuffer) : ReplicaStateMessage;
	decode(buffer: ByteBuffer) : ReplicaStateMessage;
	decode64(buffer: string) : ReplicaStateMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RangeInfoBuilder {
	new(data?: RangeInfo): RangeInfoMessage;
	decode(buffer: ArrayBuffer) : RangeInfoMessage;
	decode(buffer: ByteBuffer) : RangeInfoMessage;
	decode64(buffer: string) : RangeInfoMessage;
	
}

}




declare module cockroach {

	export interface config {

		

}

	export interface configMessage extends config {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface configBuilder {
	new(data?: config): configMessage;
	decode(buffer: ArrayBuffer) : configMessage;
	decode(buffer: ByteBuffer) : configMessage;
	decode64(buffer: string) : configMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GCPolicyBuilder {
	new(data?: GCPolicy): GCPolicyMessage;
	decode(buffer: ArrayBuffer) : GCPolicyMessage;
	decode(buffer: ByteBuffer) : GCPolicyMessage;
	decode64(buffer: string) : GCPolicyMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ZoneConfigBuilder {
	new(data?: ZoneConfig): ZoneConfigMessage;
	decode(buffer: ArrayBuffer) : ZoneConfigMessage;
	decode(buffer: ByteBuffer) : ZoneConfigMessage;
	decode64(buffer: string) : ZoneConfigMessage;
	
}

}


declare module cockroach.config {

	export interface SystemConfig {

		

values?: roachpb.KeyValue[];
		

getValues?() : roachpb.KeyValue[];
		setValues?(values : roachpb.KeyValue[]): void;
		



}

	export interface SystemConfigMessage extends SystemConfig {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SystemConfigBuilder {
	new(data?: SystemConfig): SystemConfigMessage;
	decode(buffer: ArrayBuffer) : SystemConfigMessage;
	decode(buffer: ByteBuffer) : SystemConfigMessage;
	decode64(buffer: string) : SystemConfigMessage;
	
}

}



declare module cockroach {

	export interface server {

		

}

	export interface serverMessage extends server {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface serverBuilder {
	new(data?: server): serverMessage;
	decode(buffer: ArrayBuffer) : serverMessage;
	decode(buffer: ByteBuffer) : serverMessage;
	decode64(buffer: string) : serverMessage;
	serverpb: server.serverpbBuilder;
	status: server.statusBuilder;
	
}

}

declare module cockroach.server {

	export interface serverpb {

		

}

	export interface serverpbMessage extends serverpb {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface serverpbBuilder {
	new(data?: serverpb): serverpbMessage;
	decode(buffer: ArrayBuffer) : serverpbMessage;
	decode(buffer: ByteBuffer) : serverpbMessage;
	decode64(buffer: string) : serverpbMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DatabasesRequestBuilder {
	new(data?: DatabasesRequest): DatabasesRequestMessage;
	decode(buffer: ArrayBuffer) : DatabasesRequestMessage;
	decode(buffer: ByteBuffer) : DatabasesRequestMessage;
	decode64(buffer: string) : DatabasesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DatabasesResponse {

		

databases?: string[];
		

getDatabases?() : string[];
		setDatabases?(databases : string[]): void;
		



}

	export interface DatabasesResponseMessage extends DatabasesResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DatabasesResponseBuilder {
	new(data?: DatabasesResponse): DatabasesResponseMessage;
	decode(buffer: ArrayBuffer) : DatabasesResponseMessage;
	decode(buffer: ByteBuffer) : DatabasesResponseMessage;
	decode64(buffer: string) : DatabasesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DatabaseDetailsRequest {

		

database?: string;
		

getDatabase?() : string;
		setDatabase?(database : string): void;
		



}

	export interface DatabaseDetailsRequestMessage extends DatabaseDetailsRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DatabaseDetailsRequestBuilder {
	new(data?: DatabaseDetailsRequest): DatabaseDetailsRequestMessage;
	decode(buffer: ArrayBuffer) : DatabaseDetailsRequestMessage;
	decode(buffer: ByteBuffer) : DatabaseDetailsRequestMessage;
	decode64(buffer: string) : DatabaseDetailsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DatabaseDetailsResponseBuilder {
	new(data?: DatabaseDetailsResponse): DatabaseDetailsResponseMessage;
	decode(buffer: ArrayBuffer) : DatabaseDetailsResponseMessage;
	decode(buffer: ByteBuffer) : DatabaseDetailsResponseMessage;
	decode64(buffer: string) : DatabaseDetailsResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GrantBuilder {
	new(data?: Grant): GrantMessage;
	decode(buffer: ArrayBuffer) : GrantMessage;
	decode(buffer: ByteBuffer) : GrantMessage;
	decode64(buffer: string) : GrantMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TableDetailsRequestBuilder {
	new(data?: TableDetailsRequest): TableDetailsRequestMessage;
	decode(buffer: ArrayBuffer) : TableDetailsRequestMessage;
	decode(buffer: ByteBuffer) : TableDetailsRequestMessage;
	decode64(buffer: string) : TableDetailsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TableDetailsResponseBuilder {
	new(data?: TableDetailsResponse): TableDetailsResponseMessage;
	decode(buffer: ArrayBuffer) : TableDetailsResponseMessage;
	decode(buffer: ByteBuffer) : TableDetailsResponseMessage;
	decode64(buffer: string) : TableDetailsResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GrantBuilder {
	new(data?: Grant): GrantMessage;
	decode(buffer: ArrayBuffer) : GrantMessage;
	decode(buffer: ByteBuffer) : GrantMessage;
	decode64(buffer: string) : GrantMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ColumnBuilder {
	new(data?: Column): ColumnMessage;
	decode(buffer: ArrayBuffer) : ColumnMessage;
	decode(buffer: ByteBuffer) : ColumnMessage;
	decode64(buffer: string) : ColumnMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface IndexBuilder {
	new(data?: Index): IndexMessage;
	decode(buffer: ArrayBuffer) : IndexMessage;
	decode(buffer: ByteBuffer) : IndexMessage;
	decode64(buffer: string) : IndexMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TableStatsRequestBuilder {
	new(data?: TableStatsRequest): TableStatsRequestMessage;
	decode(buffer: ArrayBuffer) : TableStatsRequestMessage;
	decode(buffer: ByteBuffer) : TableStatsRequestMessage;
	decode64(buffer: string) : TableStatsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TableStatsResponseBuilder {
	new(data?: TableStatsResponse): TableStatsResponseMessage;
	decode(buffer: ArrayBuffer) : TableStatsResponseMessage;
	decode(buffer: ByteBuffer) : TableStatsResponseMessage;
	decode64(buffer: string) : TableStatsResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface MissingNodeBuilder {
	new(data?: MissingNode): MissingNodeMessage;
	decode(buffer: ArrayBuffer) : MissingNodeMessage;
	decode(buffer: ByteBuffer) : MissingNodeMessage;
	decode64(buffer: string) : MissingNodeMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface UsersRequest {

		

}

	export interface UsersRequestMessage extends UsersRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface UsersRequestBuilder {
	new(data?: UsersRequest): UsersRequestMessage;
	decode(buffer: ArrayBuffer) : UsersRequestMessage;
	decode(buffer: ByteBuffer) : UsersRequestMessage;
	decode64(buffer: string) : UsersRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface UsersResponse {

		

users?: UsersResponse.User[];
		

getUsers?() : UsersResponse.User[];
		setUsers?(users : UsersResponse.User[]): void;
		



}

	export interface UsersResponseMessage extends UsersResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface UsersResponseBuilder {
	new(data?: UsersResponse): UsersResponseMessage;
	decode(buffer: ArrayBuffer) : UsersResponseMessage;
	decode(buffer: ByteBuffer) : UsersResponseMessage;
	decode64(buffer: string) : UsersResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface UserBuilder {
	new(data?: User): UserMessage;
	decode(buffer: ArrayBuffer) : UserMessage;
	decode(buffer: ByteBuffer) : UserMessage;
	decode64(buffer: string) : UserMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface EventsRequestBuilder {
	new(data?: EventsRequest): EventsRequestMessage;
	decode(buffer: ArrayBuffer) : EventsRequestMessage;
	decode(buffer: ByteBuffer) : EventsRequestMessage;
	decode64(buffer: string) : EventsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface EventsResponse {

		

events?: EventsResponse.Event[];
		

getEvents?() : EventsResponse.Event[];
		setEvents?(events : EventsResponse.Event[]): void;
		



}

	export interface EventsResponseMessage extends EventsResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface EventsResponseBuilder {
	new(data?: EventsResponse): EventsResponseMessage;
	decode(buffer: ArrayBuffer) : EventsResponseMessage;
	decode(buffer: ByteBuffer) : EventsResponseMessage;
	decode64(buffer: string) : EventsResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface EventBuilder {
	new(data?: Event): EventMessage;
	decode(buffer: ArrayBuffer) : EventMessage;
	decode(buffer: ByteBuffer) : EventMessage;
	decode64(buffer: string) : EventMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer) : TimestampMessage;
	decode(buffer: ByteBuffer) : TimestampMessage;
	decode64(buffer: string) : TimestampMessage;
	
}

}




declare module cockroach.server.serverpb {

	export interface SetUIDataRequest {

		

key_values?: ProtoBufMap<string, ByteBuffer>;
		

getKeyValues?() : ProtoBufMap<string, ByteBuffer>;
		setKeyValues?(keyValues : ProtoBufMap<string, ByteBuffer>): void;
		



}

	export interface SetUIDataRequestMessage extends SetUIDataRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SetUIDataRequestBuilder {
	new(data?: SetUIDataRequest): SetUIDataRequestMessage;
	decode(buffer: ArrayBuffer) : SetUIDataRequestMessage;
	decode(buffer: ByteBuffer) : SetUIDataRequestMessage;
	decode64(buffer: string) : SetUIDataRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface SetUIDataResponse {

		

}

	export interface SetUIDataResponseMessage extends SetUIDataResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SetUIDataResponseBuilder {
	new(data?: SetUIDataResponse): SetUIDataResponseMessage;
	decode(buffer: ArrayBuffer) : SetUIDataResponseMessage;
	decode(buffer: ByteBuffer) : SetUIDataResponseMessage;
	decode64(buffer: string) : SetUIDataResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GetUIDataRequest {

		

keys?: string[];
		

getKeys?() : string[];
		setKeys?(keys : string[]): void;
		



}

	export interface GetUIDataRequestMessage extends GetUIDataRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GetUIDataRequestBuilder {
	new(data?: GetUIDataRequest): GetUIDataRequestMessage;
	decode(buffer: ArrayBuffer) : GetUIDataRequestMessage;
	decode(buffer: ByteBuffer) : GetUIDataRequestMessage;
	decode64(buffer: string) : GetUIDataRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GetUIDataResponse {

		

key_values?: ProtoBufMap<string, GetUIDataResponse.Value>;
		

getKeyValues?() : ProtoBufMap<string, GetUIDataResponse.Value>;
		setKeyValues?(keyValues : ProtoBufMap<string, GetUIDataResponse.Value>): void;
		



}

	export interface GetUIDataResponseMessage extends GetUIDataResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GetUIDataResponseBuilder {
	new(data?: GetUIDataResponse): GetUIDataResponseMessage;
	decode(buffer: ArrayBuffer) : GetUIDataResponseMessage;
	decode(buffer: ByteBuffer) : GetUIDataResponseMessage;
	decode64(buffer: string) : GetUIDataResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimestampBuilder {
	new(data?: Timestamp): TimestampMessage;
	decode(buffer: ArrayBuffer) : TimestampMessage;
	decode(buffer: ByteBuffer) : TimestampMessage;
	decode64(buffer: string) : TimestampMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ValueBuilder {
	new(data?: Value): ValueMessage;
	decode(buffer: ArrayBuffer) : ValueMessage;
	decode(buffer: ByteBuffer) : ValueMessage;
	decode64(buffer: string) : ValueMessage;
	
}

}



declare module cockroach.server.serverpb {

	export interface ClusterRequest {

		

}

	export interface ClusterRequestMessage extends ClusterRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ClusterRequestBuilder {
	new(data?: ClusterRequest): ClusterRequestMessage;
	decode(buffer: ArrayBuffer) : ClusterRequestMessage;
	decode(buffer: ByteBuffer) : ClusterRequestMessage;
	decode64(buffer: string) : ClusterRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface ClusterResponse {

		

cluster_id?: string;
		

getClusterId?() : string;
		setClusterId?(clusterId : string): void;
		



}

	export interface ClusterResponseMessage extends ClusterResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ClusterResponseBuilder {
	new(data?: ClusterResponse): ClusterResponseMessage;
	decode(buffer: ArrayBuffer) : ClusterResponseMessage;
	decode(buffer: ByteBuffer) : ClusterResponseMessage;
	decode64(buffer: string) : ClusterResponseMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DrainRequestBuilder {
	new(data?: DrainRequest): DrainRequestMessage;
	decode(buffer: ArrayBuffer) : DrainRequestMessage;
	decode(buffer: ByteBuffer) : DrainRequestMessage;
	decode64(buffer: string) : DrainRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DrainResponse {

		

on?: number[];
		

getOn?() : number[];
		setOn?(on : number[]): void;
		



}

	export interface DrainResponseMessage extends DrainResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DrainResponseBuilder {
	new(data?: DrainResponse): DrainResponseMessage;
	decode(buffer: ArrayBuffer) : DrainResponseMessage;
	decode(buffer: ByteBuffer) : DrainResponseMessage;
	decode64(buffer: string) : DrainResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface HealthRequest {

		

}

	export interface HealthRequestMessage extends HealthRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface HealthRequestBuilder {
	new(data?: HealthRequest): HealthRequestMessage;
	decode(buffer: ArrayBuffer) : HealthRequestMessage;
	decode(buffer: ByteBuffer) : HealthRequestMessage;
	decode64(buffer: string) : HealthRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface HealthResponse {

		

}

	export interface HealthResponseMessage extends HealthResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface HealthResponseBuilder {
	new(data?: HealthResponse): HealthResponseMessage;
	decode(buffer: ArrayBuffer) : HealthResponseMessage;
	decode(buffer: ByteBuffer) : HealthResponseMessage;
	decode64(buffer: string) : HealthResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface ClusterFreezeRequest {

		

freeze?: boolean;
		

getFreeze?() : boolean;
		setFreeze?(freeze : boolean): void;
		



}

	export interface ClusterFreezeRequestMessage extends ClusterFreezeRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ClusterFreezeRequestBuilder {
	new(data?: ClusterFreezeRequest): ClusterFreezeRequestMessage;
	decode(buffer: ArrayBuffer) : ClusterFreezeRequestMessage;
	decode(buffer: ByteBuffer) : ClusterFreezeRequestMessage;
	decode64(buffer: string) : ClusterFreezeRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ClusterFreezeResponseBuilder {
	new(data?: ClusterFreezeResponse): ClusterFreezeResponseMessage;
	decode(buffer: ArrayBuffer) : ClusterFreezeResponseMessage;
	decode(buffer: ByteBuffer) : ClusterFreezeResponseMessage;
	decode64(buffer: string) : ClusterFreezeResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface DetailsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface DetailsRequestMessage extends DetailsRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DetailsRequestBuilder {
	new(data?: DetailsRequest): DetailsRequestMessage;
	decode(buffer: ArrayBuffer) : DetailsRequestMessage;
	decode(buffer: ByteBuffer) : DetailsRequestMessage;
	decode64(buffer: string) : DetailsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface DetailsResponseBuilder {
	new(data?: DetailsResponse): DetailsResponseMessage;
	decode(buffer: ArrayBuffer) : DetailsResponseMessage;
	decode(buffer: ByteBuffer) : DetailsResponseMessage;
	decode64(buffer: string) : DetailsResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodesRequest {

		

}

	export interface NodesRequestMessage extends NodesRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface NodesRequestBuilder {
	new(data?: NodesRequest): NodesRequestMessage;
	decode(buffer: ArrayBuffer) : NodesRequestMessage;
	decode(buffer: ByteBuffer) : NodesRequestMessage;
	decode64(buffer: string) : NodesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodesResponse {

		

nodes?: status.NodeStatus[];
		

getNodes?() : status.NodeStatus[];
		setNodes?(nodes : status.NodeStatus[]): void;
		



}

	export interface NodesResponseMessage extends NodesResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface NodesResponseBuilder {
	new(data?: NodesResponse): NodesResponseMessage;
	decode(buffer: ArrayBuffer) : NodesResponseMessage;
	decode(buffer: ByteBuffer) : NodesResponseMessage;
	decode64(buffer: string) : NodesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface NodeRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface NodeRequestMessage extends NodeRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface NodeRequestBuilder {
	new(data?: NodeRequest): NodeRequestMessage;
	decode(buffer: ArrayBuffer) : NodeRequestMessage;
	decode(buffer: ByteBuffer) : NodeRequestMessage;
	decode64(buffer: string) : NodeRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RangeInfoBuilder {
	new(data?: RangeInfo): RangeInfoMessage;
	decode(buffer: ArrayBuffer) : RangeInfoMessage;
	decode(buffer: ByteBuffer) : RangeInfoMessage;
	decode64(buffer: string) : RangeInfoMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RangesRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface RangesRequestMessage extends RangesRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RangesRequestBuilder {
	new(data?: RangesRequest): RangesRequestMessage;
	decode(buffer: ArrayBuffer) : RangesRequestMessage;
	decode(buffer: ByteBuffer) : RangesRequestMessage;
	decode64(buffer: string) : RangesRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RangesResponse {

		

ranges?: RangeInfo[];
		

getRanges?() : RangeInfo[];
		setRanges?(ranges : RangeInfo[]): void;
		



}

	export interface RangesResponseMessage extends RangesResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RangesResponseBuilder {
	new(data?: RangesResponse): RangesResponseMessage;
	decode(buffer: ArrayBuffer) : RangesResponseMessage;
	decode(buffer: ByteBuffer) : RangesResponseMessage;
	decode64(buffer: string) : RangesResponseMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface GossipRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface GossipRequestMessage extends GossipRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface GossipRequestBuilder {
	new(data?: GossipRequest): GossipRequestMessage;
	decode(buffer: ArrayBuffer) : GossipRequestMessage;
	decode(buffer: ByteBuffer) : GossipRequestMessage;
	decode64(buffer: string) : GossipRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface JSONResponse {

		

data?: ByteBuffer;
		

getData?() : ByteBuffer;
		setData?(data : ByteBuffer): void;
		



}

	export interface JSONResponseMessage extends JSONResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface JSONResponseBuilder {
	new(data?: JSONResponse): JSONResponseMessage;
	decode(buffer: ArrayBuffer) : JSONResponseMessage;
	decode(buffer: ByteBuffer) : JSONResponseMessage;
	decode64(buffer: string) : JSONResponseMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface LogsRequestBuilder {
	new(data?: LogsRequest): LogsRequestMessage;
	decode(buffer: ArrayBuffer) : LogsRequestMessage;
	decode(buffer: ByteBuffer) : LogsRequestMessage;
	decode64(buffer: string) : LogsRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface LogFilesListRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface LogFilesListRequestMessage extends LogFilesListRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface LogFilesListRequestBuilder {
	new(data?: LogFilesListRequest): LogFilesListRequestMessage;
	decode(buffer: ArrayBuffer) : LogFilesListRequestMessage;
	decode(buffer: ByteBuffer) : LogFilesListRequestMessage;
	decode64(buffer: string) : LogFilesListRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface LogFileRequestBuilder {
	new(data?: LogFileRequest): LogFileRequestMessage;
	decode(buffer: ArrayBuffer) : LogFileRequestMessage;
	decode(buffer: ByteBuffer) : LogFileRequestMessage;
	decode64(buffer: string) : LogFileRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface StacksRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface StacksRequestMessage extends StacksRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StacksRequestBuilder {
	new(data?: StacksRequest): StacksRequestMessage;
	decode(buffer: ArrayBuffer) : StacksRequestMessage;
	decode(buffer: ByteBuffer) : StacksRequestMessage;
	decode64(buffer: string) : StacksRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface MetricsRequest {

		

node_id?: string;
		

getNodeId?() : string;
		setNodeId?(nodeId : string): void;
		



}

	export interface MetricsRequestMessage extends MetricsRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface MetricsRequestBuilder {
	new(data?: MetricsRequest): MetricsRequestMessage;
	decode(buffer: ArrayBuffer) : MetricsRequestMessage;
	decode(buffer: ByteBuffer) : MetricsRequestMessage;
	decode64(buffer: string) : MetricsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftRangeNodeBuilder {
	new(data?: RaftRangeNode): RaftRangeNodeMessage;
	decode(buffer: ArrayBuffer) : RaftRangeNodeMessage;
	decode(buffer: ByteBuffer) : RaftRangeNodeMessage;
	decode64(buffer: string) : RaftRangeNodeMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftRangeError {

		

message?: string;
		

getMessage?() : string;
		setMessage?(message : string): void;
		



}

	export interface RaftRangeErrorMessage extends RaftRangeError {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftRangeErrorBuilder {
	new(data?: RaftRangeError): RaftRangeErrorMessage;
	decode(buffer: ArrayBuffer) : RaftRangeErrorMessage;
	decode(buffer: ByteBuffer) : RaftRangeErrorMessage;
	decode64(buffer: string) : RaftRangeErrorMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftRangeStatusBuilder {
	new(data?: RaftRangeStatus): RaftRangeStatusMessage;
	decode(buffer: ArrayBuffer) : RaftRangeStatusMessage;
	decode(buffer: ByteBuffer) : RaftRangeStatusMessage;
	decode64(buffer: string) : RaftRangeStatusMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftDebugRequest {

		

}

	export interface RaftDebugRequestMessage extends RaftDebugRequest {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftDebugRequestBuilder {
	new(data?: RaftDebugRequest): RaftDebugRequestMessage;
	decode(buffer: ArrayBuffer) : RaftDebugRequestMessage;
	decode(buffer: ByteBuffer) : RaftDebugRequestMessage;
	decode64(buffer: string) : RaftDebugRequestMessage;
	
}

}


declare module cockroach.server.serverpb {

	export interface RaftDebugResponse {

		

ranges?: ProtoBufMap<Long, RaftRangeStatus>;
		

getRanges?() : ProtoBufMap<Long, RaftRangeStatus>;
		setRanges?(ranges : ProtoBufMap<Long, RaftRangeStatus>): void;
		



}

	export interface RaftDebugResponseMessage extends RaftDebugResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RaftDebugResponseBuilder {
	new(data?: RaftDebugResponse): RaftDebugResponseMessage;
	decode(buffer: ArrayBuffer) : RaftDebugResponseMessage;
	decode(buffer: ByteBuffer) : RaftDebugResponseMessage;
	decode64(buffer: string) : RaftDebugResponseMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SpanStatsRequestBuilder {
	new(data?: SpanStatsRequest): SpanStatsRequestMessage;
	decode(buffer: ArrayBuffer) : SpanStatsRequestMessage;
	decode(buffer: ByteBuffer) : SpanStatsRequestMessage;
	decode64(buffer: string) : SpanStatsRequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface SpanStatsResponseBuilder {
	new(data?: SpanStatsResponse): SpanStatsResponseMessage;
	decode(buffer: ArrayBuffer) : SpanStatsResponseMessage;
	decode(buffer: ByteBuffer) : SpanStatsResponseMessage;
	decode64(buffer: string) : SpanStatsResponseMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface PrettySpanBuilder {
	new(data?: PrettySpan): PrettySpanMessage;
	decode(buffer: ArrayBuffer) : PrettySpanMessage;
	decode(buffer: ByteBuffer) : PrettySpanMessage;
	decode64(buffer: string) : PrettySpanMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface statusBuilder {
	new(data?: status): statusMessage;
	decode(buffer: ArrayBuffer) : statusMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface StoreStatusBuilder {
	new(data?: StoreStatus): StoreStatusMessage;
	decode(buffer: ArrayBuffer) : StoreStatusMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface NodeStatusBuilder {
	new(data?: NodeStatus): NodeStatusMessage;
	decode(buffer: ArrayBuffer) : NodeStatusMessage;
	decode(buffer: ByteBuffer) : NodeStatusMessage;
	decode64(buffer: string) : NodeStatusMessage;
	
}

}




declare module cockroach {

	export interface build {

		

}

	export interface buildMessage extends build {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface buildBuilder {
	new(data?: build): buildMessage;
	decode(buffer: ArrayBuffer) : buildMessage;
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
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface InfoBuilder {
	new(data?: Info): InfoMessage;
	decode(buffer: ArrayBuffer) : InfoMessage;
	decode(buffer: ByteBuffer) : InfoMessage;
	decode64(buffer: string) : InfoMessage;
	
}

}



declare module cockroach {

	export interface gossip {

		

}

	export interface gossipMessage extends gossip {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface gossipBuilder {
	new(data?: gossip): gossipMessage;
	decode(buffer: ArrayBuffer) : gossipMessage;
	decode(buffer: ByteBuffer) : gossipMessage;
	decode64(buffer: string) : gossipMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface BootstrapInfoBuilder {
	new(data?: BootstrapInfo): BootstrapInfoMessage;
	decode(buffer: ArrayBuffer) : BootstrapInfoMessage;
	decode(buffer: ByteBuffer) : BootstrapInfoMessage;
	decode64(buffer: string) : BootstrapInfoMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface RequestBuilder {
	new(data?: Request): RequestMessage;
	decode(buffer: ArrayBuffer) : RequestMessage;
	decode(buffer: ByteBuffer) : RequestMessage;
	decode64(buffer: string) : RequestMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ResponseBuilder {
	new(data?: Response): ResponseMessage;
	decode(buffer: ArrayBuffer) : ResponseMessage;
	decode(buffer: ByteBuffer) : ResponseMessage;
	decode64(buffer: string) : ResponseMessage;
	
}

}


declare module cockroach.gossip {

	export interface InfoStatus {

		

infos?: ProtoBufMap<string, Info>;
		

getInfos?() : ProtoBufMap<string, Info>;
		setInfos?(infos : ProtoBufMap<string, Info>): void;
		



}

	export interface InfoStatusMessage extends InfoStatus {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface InfoStatusBuilder {
	new(data?: InfoStatus): InfoStatusMessage;
	decode(buffer: ArrayBuffer) : InfoStatusMessage;
	decode(buffer: ByteBuffer) : InfoStatusMessage;
	decode64(buffer: string) : InfoStatusMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface InfoBuilder {
	new(data?: Info): InfoMessage;
	decode(buffer: ArrayBuffer) : InfoMessage;
	decode(buffer: ByteBuffer) : InfoMessage;
	decode64(buffer: string) : InfoMessage;
	
}

}



declare module cockroach {

	export interface ts {

		

}

	export interface tsMessage extends ts {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface tsBuilder {
	new(data?: ts): tsMessage;
	decode(buffer: ArrayBuffer) : tsMessage;
	decode(buffer: ByteBuffer) : tsMessage;
	decode64(buffer: string) : tsMessage;
	tspb: ts.tspbBuilder;
	
}

}

declare module cockroach.ts {

	export interface tspb {

		

}

	export interface tspbMessage extends tspb {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface tspbBuilder {
	new(data?: tspb): tspbMessage;
	decode(buffer: ArrayBuffer) : tspbMessage;
	decode(buffer: ByteBuffer) : tspbMessage;
	decode64(buffer: string) : tspbMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesDatapointBuilder {
	new(data?: TimeSeriesDatapoint): TimeSeriesDatapointMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesDatapointMessage;
	decode(buffer: ByteBuffer) : TimeSeriesDatapointMessage;
	decode64(buffer: string) : TimeSeriesDatapointMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesDataBuilder {
	new(data?: TimeSeriesData): TimeSeriesDataMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesDataMessage;
	decode(buffer: ByteBuffer) : TimeSeriesDataMessage;
	decode64(buffer: string) : TimeSeriesDataMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface QueryBuilder {
	new(data?: Query): QueryMessage;
	decode(buffer: ArrayBuffer) : QueryMessage;
	decode(buffer: ByteBuffer) : QueryMessage;
	decode64(buffer: string) : QueryMessage;
	
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesQueryRequestBuilder {
	new(data?: TimeSeriesQueryRequest): TimeSeriesQueryRequestMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesQueryRequestMessage;
	decode(buffer: ByteBuffer) : TimeSeriesQueryRequestMessage;
	decode64(buffer: string) : TimeSeriesQueryRequestMessage;
	
}

}


declare module cockroach.ts.tspb {

	export interface TimeSeriesQueryResponse {

		

results?: TimeSeriesQueryResponse.Result[];
		

getResults?() : TimeSeriesQueryResponse.Result[];
		setResults?(results : TimeSeriesQueryResponse.Result[]): void;
		



}

	export interface TimeSeriesQueryResponseMessage extends TimeSeriesQueryResponse {
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface TimeSeriesQueryResponseBuilder {
	new(data?: TimeSeriesQueryResponse): TimeSeriesQueryResponseMessage;
	decode(buffer: ArrayBuffer) : TimeSeriesQueryResponseMessage;
	decode(buffer: ByteBuffer) : TimeSeriesQueryResponseMessage;
	decode64(buffer: string) : TimeSeriesQueryResponseMessage;
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
	toArrayBuffer(): ArrayBuffer;
	encode(): ByteBuffer;
	encodeJSON(): string;
	toBase64(): string;
	toString(): string;
}

export interface ResultBuilder {
	new(data?: Result): ResultMessage;
	decode(buffer: ArrayBuffer) : ResultMessage;
	decode(buffer: ByteBuffer) : ResultMessage;
	decode64(buffer: string) : ResultMessage;
	
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




