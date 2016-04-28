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
	DatabasesRequest: server.DatabasesRequestBuilder;
	DatabasesResponse: server.DatabasesResponseBuilder;
	DatabaseDetailsRequest: server.DatabaseDetailsRequestBuilder;
	DatabaseDetailsResponse: server.DatabaseDetailsResponseBuilder;
	TableDetailsRequest: server.TableDetailsRequestBuilder;
	TableDetailsResponse: server.TableDetailsResponseBuilder;
	UsersRequest: server.UsersRequestBuilder;
	UsersResponse: server.UsersResponseBuilder;
	EventsRequest: server.EventsRequestBuilder;
	EventsResponse: server.EventsResponseBuilder;
	SetUIDataRequest: server.SetUIDataRequestBuilder;
	SetUIDataResponse: server.SetUIDataResponseBuilder;
	GetUIDataRequest: server.GetUIDataRequestBuilder;
	GetUIDataResponse: server.GetUIDataResponseBuilder;
	ClusterRequest: server.ClusterRequestBuilder;
	ClusterResponse: server.ClusterResponseBuilder;
	DrainRequest: server.DrainRequestBuilder;
	DrainResponse: server.DrainResponseBuilder;
	ClusterFreezeRequest: server.ClusterFreezeRequestBuilder;
	ClusterFreezeResponse: server.ClusterFreezeResponseBuilder;
	status: server.statusBuilder;
	DrainMode: server.DrainMode;
	
}
	
}

declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
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

declare module cockroach.server.DatabaseDetailsResponse {
	
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



declare module cockroach.server {
	
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


declare module cockroach.server {
	
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

declare module cockroach.server.TableDetailsResponse {
	
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


declare module cockroach.server.TableDetailsResponse {
	
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


declare module cockroach.server.TableDetailsResponse {
	
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



declare module cockroach.server {
	
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


declare module cockroach.server {
	
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

declare module cockroach.server.UsersResponse {
	
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



declare module cockroach.server {
	
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


declare module cockroach.server {
	
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

declare module cockroach.server.EventsResponse {
	
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

declare module cockroach.server.EventsResponse.Event {
	
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




declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
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

declare module cockroach.server.GetUIDataResponse {
	
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


declare module cockroach.server.GetUIDataResponse {
	
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



declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
	export interface DrainRequest {
	
		

on?: number[];
		

getOn?() : number[];
		setOn?(on : number[]): void;
		



off?: number[];
		

getOff?() : number[];
		setOff?(off : number[]): void;
		



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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
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


declare module cockroach.server {
	
	export interface ClusterFreezeResponse {
	
		

ranges_affected?: Long;
		

getRangesAffected?() : Long;
		setRangesAffected?(rangesAffected : Long): void;
		



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



declare module cockroach.server {
	export const enum DrainMode {
		CLIENT = 0,
		LEADERSHIP = 1,
		
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


declare module cockroach.ts {
	
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



