package sarama

type offsetRequestBlock struct {
	time       int64
	maxOffsets int32 // Only used in version 0
}

func (b *offsetRequestBlock) encode(pe packetEncoder, version int16) error {
	pe.putInt64(b.time)
	if version == 0 {
		pe.putInt32(b.maxOffsets)
	}

	return nil
}

func (b *offsetRequestBlock) decode(pd packetDecoder, version int16) (err error) {
	if b.time, err = pd.getInt64(); err != nil {
		return err
	}
	if version == 0 {
		if b.maxOffsets, err = pd.getInt32(); err != nil {
			return err
		}
	}
	return nil
}

type OffsetRequest struct {
	Version        int16
	IsolationLevel IsolationLevel
	replicaID      int32
	isReplicaIDSet bool
	blocks         map[string]map[int32]*offsetRequestBlock
}

func (r *OffsetRequest) encode(pe packetEncoder) error {
	if r.isReplicaIDSet {
		pe.putInt32(r.replicaID)
	} else {
		// default replica ID is always -1 for clients
		pe.putInt32(-1)
	}

	if r.Version >= 2 {
		pe.putBool(r.IsolationLevel == ReadCommitted)
	}

	err := pe.putArrayLength(len(r.blocks))
	if err != nil {
		return err
	}
	for topic, partitions := range r.blocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(partitions))
		if err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err = block.encode(pe, r.Version); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *OffsetRequest) decode(pd packetDecoder, version int16) error {
	r.Version = version

	replicaID, err := pd.getInt32()
	if err != nil {
		return err
	}
	if replicaID >= 0 {
		r.SetReplicaID(replicaID)
	}

	if r.Version >= 2 {
		tmp, err := pd.getBool()
		if err != nil {
			return err
		}

		r.IsolationLevel = ReadUncommitted
		if tmp {
			r.IsolationLevel = ReadCommitted
		}
	}

	blockCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if blockCount == 0 {
		return nil
	}
	r.blocks = make(map[string]map[int32]*offsetRequestBlock)
	for i := 0; i < blockCount; i++ {
		topic, err := pd.getString()
		if err != nil {
			return err
		}
		partitionCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		r.blocks[topic] = make(map[int32]*offsetRequestBlock)
		for j := 0; j < partitionCount; j++ {
			partition, err := pd.getInt32()
			if err != nil {
				return err
			}
			block := &offsetRequestBlock{}
			if err := block.decode(pd, version); err != nil {
				return err
			}
			r.blocks[topic][partition] = block
		}
	}
	return nil
}

func (r *OffsetRequest) key() int16 {
	return 2
}

func (r *OffsetRequest) version() int16 {
	return r.Version
}

func (r *OffsetRequest) headerVersion() int16 {
	return 1
}

func (r *OffsetRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_10_1_0
	case 2:
		return V0_11_0_0
	default:
		return MinVersion
	}
}

func (r *OffsetRequest) SetReplicaID(id int32) {
	r.replicaID = id
	r.isReplicaIDSet = true
}

func (r *OffsetRequest) ReplicaID() int32 {
	if r.isReplicaIDSet {
		return r.replicaID
	}
	return -1
}

func (r *OffsetRequest) AddBlock(topic string, partitionID int32, time int64, maxOffsets int32) {
	if r.blocks == nil {
		r.blocks = make(map[string]map[int32]*offsetRequestBlock)
	}

	if r.blocks[topic] == nil {
		r.blocks[topic] = make(map[int32]*offsetRequestBlock)
	}

	tmp := new(offsetRequestBlock)
	tmp.time = time
	if r.Version == 0 {
		tmp.maxOffsets = maxOffsets
	}

	r.blocks[topic][partitionID] = tmp
}
