package sarama

import "time"

type ScramMechanismType int8

const (
	SCRAM_MECHANISM_UNKNOWN ScramMechanismType = iota // 0
	SCRAM_MECHANISM_SHA_256                           // 1
	SCRAM_MECHANISM_SHA_512                           // 2
)

func (s ScramMechanismType) String() string {
	switch s {
	case 1:
		return SASLTypeSCRAMSHA256
	case 2:
		return SASLTypeSCRAMSHA512
	default:
		return "Unknown"
	}
}

type DescribeUserScramCredentialsResponse struct {
	// Version 0 is currently only supported
	Version int16

	ThrottleTime time.Duration

	ErrorCode    KError
	ErrorMessage *string

	Results []*DescribeUserScramCredentialsResult
}

type DescribeUserScramCredentialsResult struct {
	User string

	ErrorCode    KError
	ErrorMessage *string

	CredentialInfos []*UserScramCredentialsResponseInfo
}

type UserScramCredentialsResponseInfo struct {
	Mechanism  ScramMechanismType
	Iterations int32
}

func (r *DescribeUserScramCredentialsResponse) encode(pe packetEncoder) error {
	pe.putInt32(int32(r.ThrottleTime / time.Millisecond))

	pe.putInt16(int16(r.ErrorCode))
	if err := pe.putNullableCompactString(r.ErrorMessage); err != nil {
		return err
	}

	pe.putCompactArrayLength(len(r.Results))
	for _, u := range r.Results {
		if err := pe.putCompactString(u.User); err != nil {
			return err
		}
		pe.putInt16(int16(u.ErrorCode))
		if err := pe.putNullableCompactString(u.ErrorMessage); err != nil {
			return err
		}

		pe.putCompactArrayLength(len(u.CredentialInfos))
		for _, c := range u.CredentialInfos {
			pe.putInt8(int8(c.Mechanism))
			pe.putInt32(c.Iterations)
			pe.putEmptyTaggedFieldArray()
		}

		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *DescribeUserScramCredentialsResponse) decode(pd packetDecoder, version int16) error {
	throttleTime, err := pd.getInt32()
	if err != nil {
		return err
	}
	r.ThrottleTime = time.Duration(throttleTime) * time.Millisecond

	kerr, err := pd.getInt16()
	if err != nil {
		return err
	}

	r.ErrorCode = KError(kerr)
	if r.ErrorMessage, err = pd.getCompactNullableString(); err != nil {
		return err
	}

	numUsers, err := pd.getCompactArrayLength()
	if err != nil {
		return err
	}

	if numUsers > 0 {
		r.Results = make([]*DescribeUserScramCredentialsResult, numUsers)
		for i := 0; i < numUsers; i++ {
			r.Results[i] = &DescribeUserScramCredentialsResult{}
			if r.Results[i].User, err = pd.getCompactString(); err != nil {
				return err
			}

			errorCode, err := pd.getInt16()
			if err != nil {
				return err
			}
			r.Results[i].ErrorCode = KError(errorCode)
			if r.Results[i].ErrorMessage, err = pd.getCompactNullableString(); err != nil {
				return err
			}

			numCredentialInfos, err := pd.getCompactArrayLength()
			if err != nil {
				return err
			}

			r.Results[i].CredentialInfos = make([]*UserScramCredentialsResponseInfo, numCredentialInfos)
			for j := 0; j < numCredentialInfos; j++ {
				r.Results[i].CredentialInfos[j] = &UserScramCredentialsResponseInfo{}
				scramMechanism, err := pd.getInt8()
				if err != nil {
					return err
				}
				r.Results[i].CredentialInfos[j].Mechanism = ScramMechanismType(scramMechanism)
				if r.Results[i].CredentialInfos[j].Iterations, err = pd.getInt32(); err != nil {
					return err
				}
				if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
					return err
				}
			}

			if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
				return err
			}
		}
	}

	if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}
	return nil
}

func (r *DescribeUserScramCredentialsResponse) key() int16 {
	return 50
}

func (r *DescribeUserScramCredentialsResponse) version() int16 {
	return r.Version
}

func (r *DescribeUserScramCredentialsResponse) headerVersion() int16 {
	return 2
}

func (r *DescribeUserScramCredentialsResponse) requiredVersion() KafkaVersion {
	return V2_7_0_0
}
