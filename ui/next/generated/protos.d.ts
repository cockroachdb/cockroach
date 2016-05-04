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





