package sarama

type AlterUserScramCredentialsRequest struct {
	Version int16

	// Deletions represent list of SCRAM credentials to remove
	Deletions []AlterUserScramCredentialsDelete

	// Upsertions represent list of SCRAM credentials to update/insert
	Upsertions []AlterUserScramCredentialsUpsert
}

type AlterUserScramCredentialsDelete struct {
	Name      string
	Mechanism ScramMechanismType
}

type AlterUserScramCredentialsUpsert struct {
	Name           string
	Mechanism      ScramMechanismType
	Iterations     int32
	Salt           []byte
	saltedPassword []byte

	// This field is never transmitted over the wire
	// @see: https://tools.ietf.org/html/rfc5802
	Password []byte
}

func (r *AlterUserScramCredentialsRequest) encode(pe packetEncoder) error {
	pe.putCompactArrayLength(len(r.Deletions))
	for _, d := range r.Deletions {
		if err := pe.putCompactString(d.Name); err != nil {
			return err
		}
		pe.putInt8(int8(d.Mechanism))
		pe.putEmptyTaggedFieldArray()
	}

	pe.putCompactArrayLength(len(r.Upsertions))
	for _, u := range r.Upsertions {
		if err := pe.putCompactString(u.Name); err != nil {
			return err
		}
		pe.putInt8(int8(u.Mechanism))
		pe.putInt32(u.Iterations)

		if err := pe.putCompactBytes(u.Salt); err != nil {
			return err
		}

		// do not transmit the password over the wire
		formatter := scramFormatter{mechanism: u.Mechanism}
		salted, err := formatter.saltedPassword(u.Password, u.Salt, int(u.Iterations))
		if err != nil {
			return err
		}

		if err := pe.putCompactBytes(salted); err != nil {
			return err
		}
		pe.putEmptyTaggedFieldArray()
	}

	pe.putEmptyTaggedFieldArray()
	return nil
}

func (r *AlterUserScramCredentialsRequest) decode(pd packetDecoder, version int16) error {
	numDeletions, err := pd.getCompactArrayLength()
	if err != nil {
		return err
	}

	r.Deletions = make([]AlterUserScramCredentialsDelete, numDeletions)
	for i := 0; i < numDeletions; i++ {
		r.Deletions[i] = AlterUserScramCredentialsDelete{}
		if r.Deletions[i].Name, err = pd.getCompactString(); err != nil {
			return err
		}
		mechanism, err := pd.getInt8()
		if err != nil {
			return err
		}
		r.Deletions[i].Mechanism = ScramMechanismType(mechanism)
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	numUpsertions, err := pd.getCompactArrayLength()
	if err != nil {
		return err
	}

	r.Upsertions = make([]AlterUserScramCredentialsUpsert, numUpsertions)
	for i := 0; i < numUpsertions; i++ {
		r.Upsertions[i] = AlterUserScramCredentialsUpsert{}
		if r.Upsertions[i].Name, err = pd.getCompactString(); err != nil {
			return err
		}
		mechanism, err := pd.getInt8()
		if err != nil {
			return err
		}

		r.Upsertions[i].Mechanism = ScramMechanismType(mechanism)
		if r.Upsertions[i].Iterations, err = pd.getInt32(); err != nil {
			return err
		}
		if r.Upsertions[i].Salt, err = pd.getCompactBytes(); err != nil {
			return err
		}
		if r.Upsertions[i].saltedPassword, err = pd.getCompactBytes(); err != nil {
			return err
		}
		if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
			return err
		}
	}

	if _, err = pd.getEmptyTaggedFieldArray(); err != nil {
		return err
	}
	return nil
}

func (r *AlterUserScramCredentialsRequest) key() int16 {
	return 51
}

func (r *AlterUserScramCredentialsRequest) version() int16 {
	return r.Version
}

func (r *AlterUserScramCredentialsRequest) headerVersion() int16 {
	return 2
}

func (r *AlterUserScramCredentialsRequest) requiredVersion() KafkaVersion {
	return V2_7_0_0
}
