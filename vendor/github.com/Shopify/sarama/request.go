package sarama

import (
	"encoding/binary"
	"fmt"
	"io"
)

type protocolBody interface {
	encoder
	versionedDecoder
	key() int16
	version() int16
	headerVersion() int16
	requiredVersion() KafkaVersion
}

type request struct {
	correlationID int32
	clientID      string
	body          protocolBody
}

func (r *request) encode(pe packetEncoder) error {
	pe.push(&lengthField{})
	pe.putInt16(r.body.key())
	pe.putInt16(r.body.version())
	pe.putInt32(r.correlationID)

	if r.body.headerVersion() >= 1 {
		err := pe.putString(r.clientID)
		if err != nil {
			return err
		}
	}

	if r.body.headerVersion() >= 2 {
		// we don't use tag headers at the moment so we just put an array length of 0
		pe.putUVarint(0)
	}

	err := r.body.encode(pe)
	if err != nil {
		return err
	}

	return pe.pop()
}

func (r *request) decode(pd packetDecoder) (err error) {
	key, err := pd.getInt16()
	if err != nil {
		return err
	}

	version, err := pd.getInt16()
	if err != nil {
		return err
	}

	r.correlationID, err = pd.getInt32()
	if err != nil {
		return err
	}

	r.clientID, err = pd.getString()
	if err != nil {
		return err
	}

	r.body = allocateBody(key, version)
	if r.body == nil {
		return PacketDecodingError{fmt.Sprintf("unknown request key (%d)", key)}
	}

	if r.body.headerVersion() >= 2 {
		// tagged field
		_, err = pd.getUVarint()
		if err != nil {
			return err
		}
	}

	return r.body.decode(pd, version)
}

func decodeRequest(r io.Reader) (*request, int, error) {
	var (
		bytesRead   int
		lengthBytes = make([]byte, 4)
	)

	if _, err := io.ReadFull(r, lengthBytes); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(lengthBytes)
	length := int32(binary.BigEndian.Uint32(lengthBytes))

	if length <= 4 || length > MaxRequestSize {
		return nil, bytesRead, PacketDecodingError{fmt.Sprintf("message of length %d too large or too small", length)}
	}

	encodedReq := make([]byte, length)
	if _, err := io.ReadFull(r, encodedReq); err != nil {
		return nil, bytesRead, err
	}

	bytesRead += len(encodedReq)

	req := &request{}
	if err := decode(encodedReq, req); err != nil {
		return nil, bytesRead, err
	}

	return req, bytesRead, nil
}

func allocateBody(key, version int16) protocolBody {
	switch key {
	case 0:
		return &ProduceRequest{}
	case 1:
		return &FetchRequest{Version: version}
	case 2:
		return &OffsetRequest{Version: version}
	case 3:
		return &MetadataRequest{}
	case 8:
		return &OffsetCommitRequest{Version: version}
	case 9:
		return &OffsetFetchRequest{Version: version}
	case 10:
		return &FindCoordinatorRequest{}
	case 11:
		return &JoinGroupRequest{}
	case 12:
		return &HeartbeatRequest{}
	case 13:
		return &LeaveGroupRequest{}
	case 14:
		return &SyncGroupRequest{}
	case 15:
		return &DescribeGroupsRequest{}
	case 16:
		return &ListGroupsRequest{}
	case 17:
		return &SaslHandshakeRequest{}
	case 18:
		return &ApiVersionsRequest{}
	case 19:
		return &CreateTopicsRequest{}
	case 20:
		return &DeleteTopicsRequest{}
	case 21:
		return &DeleteRecordsRequest{}
	case 22:
		return &InitProducerIDRequest{}
	case 24:
		return &AddPartitionsToTxnRequest{}
	case 25:
		return &AddOffsetsToTxnRequest{}
	case 26:
		return &EndTxnRequest{}
	case 28:
		return &TxnOffsetCommitRequest{}
	case 29:
		return &DescribeAclsRequest{}
	case 30:
		return &CreateAclsRequest{}
	case 31:
		return &DeleteAclsRequest{}
	case 32:
		return &DescribeConfigsRequest{}
	case 33:
		return &AlterConfigsRequest{}
	case 35:
		return &DescribeLogDirsRequest{}
	case 36:
		return &SaslAuthenticateRequest{}
	case 37:
		return &CreatePartitionsRequest{}
	case 42:
		return &DeleteGroupsRequest{}
	case 45:
		return &AlterPartitionReassignmentsRequest{}
	case 46:
		return &ListPartitionReassignmentsRequest{}
	case 50:
		return &DescribeUserScramCredentialsRequest{}
	case 51:
		return &AlterUserScramCredentialsRequest{}
	}
	return nil
}
