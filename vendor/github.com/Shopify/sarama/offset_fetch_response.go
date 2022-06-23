package sarama

type OffsetFetchResponseBlock struct {
	Offset      int64
	LeaderEpoch int32
	Metadata    string
	Err         KError
}

func (b *OffsetFetchResponseBlock) decode(pd packetDecoder, version int16) (err error) {
	isFlexible := version >= 6

	b.Offset, err = pd.getInt64()
	if err != nil {
		return err
	}

	if version >= 5 {
		b.LeaderEpoch, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	if isFlexible {
		b.Metadata, err = pd.getCompactString()
	} else {
		b.Metadata, err = pd.getString()
	}
	if err != nil {
		return err
	}

	tmp, err := pd.getInt16()
	if err != nil {
		return err
	}
	b.Err = KError(tmp)

	if isFlexible {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return nil
}

func (b *OffsetFetchResponseBlock) encode(pe packetEncoder, version int16) (err error) {
	isFlexible := version >= 6
	pe.putInt64(b.Offset)

	if version >= 5 {
		pe.putInt32(b.LeaderEpoch)
	}
	if isFlexible {
		err = pe.putCompactString(b.Metadata)
	} else {
		err = pe.putString(b.Metadata)
	}
	if err != nil {
		return err
	}

	pe.putInt16(int16(b.Err))

	if isFlexible {
		pe.putEmptyTaggedFieldArray()
	}

	return nil
}

type OffsetFetchResponse struct {
	Version        int16
	ThrottleTimeMs int32
	Blocks         map[string]map[int32]*OffsetFetchResponseBlock
	Err            KError
}

func (r *OffsetFetchResponse) encode(pe packetEncoder) (err error) {
	isFlexible := r.Version >= 6

	if r.Version >= 3 {
		pe.putInt32(r.ThrottleTimeMs)
	}
	if isFlexible {
		pe.putCompactArrayLength(len(r.Blocks))
	} else {
		err = pe.putArrayLength(len(r.Blocks))
	}
	if err != nil {
		return err
	}

	for topic, partitions := range r.Blocks {
		if isFlexible {
			err = pe.putCompactString(topic)
		} else {
			err = pe.putString(topic)
		}
		if err != nil {
			return err
		}

		if isFlexible {
			pe.putCompactArrayLength(len(partitions))
		} else {
			err = pe.putArrayLength(len(partitions))
		}
		if err != nil {
			return err
		}
		for partition, block := range partitions {
			pe.putInt32(partition)
			if err := block.encode(pe, r.Version); err != nil {
				return err
			}
		}
		if isFlexible {
			pe.putEmptyTaggedFieldArray()
		}
	}
	if r.Version >= 2 {
		pe.putInt16(int16(r.Err))
	}
	if isFlexible {
		pe.putEmptyTaggedFieldArray()
	}
	return nil
}

func (r *OffsetFetchResponse) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	isFlexible := version >= 6

	if version >= 3 {
		r.ThrottleTimeMs, err = pd.getInt32()
		if err != nil {
			return err
		}
	}

	var numTopics int
	if isFlexible {
		numTopics, err = pd.getCompactArrayLength()
	} else {
		numTopics, err = pd.getArrayLength()
	}
	if err != nil {
		return err
	}

	if numTopics > 0 {
		r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock, numTopics)
		for i := 0; i < numTopics; i++ {
			var name string
			if isFlexible {
				name, err = pd.getCompactString()
			} else {
				name, err = pd.getString()
			}
			if err != nil {
				return err
			}

			var numBlocks int
			if isFlexible {
				numBlocks, err = pd.getCompactArrayLength()
			} else {
				numBlocks, err = pd.getArrayLength()
			}
			if err != nil {
				return err
			}

			r.Blocks[name] = nil
			if numBlocks > 0 {
				r.Blocks[name] = make(map[int32]*OffsetFetchResponseBlock, numBlocks)
			}
			for j := 0; j < numBlocks; j++ {
				id, err := pd.getInt32()
				if err != nil {
					return err
				}

				block := new(OffsetFetchResponseBlock)
				err = block.decode(pd, version)
				if err != nil {
					return err
				}

				r.Blocks[name][id] = block
			}

			if isFlexible {
				if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
					return err
				}
			}
		}
	}

	if version >= 2 {
		kerr, err := pd.getInt16()
		if err != nil {
			return err
		}
		r.Err = KError(kerr)
	}

	if isFlexible {
		if _, err := pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	return nil
}

func (r *OffsetFetchResponse) key() int16 {
	return 9
}

func (r *OffsetFetchResponse) version() int16 {
	return r.Version
}

func (r *OffsetFetchResponse) headerVersion() int16 {
	if r.Version >= 6 {
		return 1
	}

	return 0
}

func (r *OffsetFetchResponse) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_8_2_0
	case 2:
		return V0_10_2_0
	case 3:
		return V0_11_0_0
	case 4:
		return V2_0_0_0
	case 5:
		return V2_1_0_0
	case 6:
		return V2_4_0_0
	case 7:
		return V2_5_0_0
	default:
		return MinVersion
	}
}

func (r *OffsetFetchResponse) GetBlock(topic string, partition int32) *OffsetFetchResponseBlock {
	if r.Blocks == nil {
		return nil
	}

	if r.Blocks[topic] == nil {
		return nil
	}

	return r.Blocks[topic][partition]
}

func (r *OffsetFetchResponse) AddBlock(topic string, partition int32, block *OffsetFetchResponseBlock) {
	if r.Blocks == nil {
		r.Blocks = make(map[string]map[int32]*OffsetFetchResponseBlock)
	}
	partitions := r.Blocks[topic]
	if partitions == nil {
		partitions = make(map[int32]*OffsetFetchResponseBlock)
		r.Blocks[topic] = partitions
	}
	partitions[partition] = block
}
