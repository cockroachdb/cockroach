package aws

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
)

const sshPublicKeyFile = "${HOME}/.ssh/id_rsa.pub"

// sshKeyExists checks to see if there is a an SSH key with the given name in the given region.
func sshKeyExists(keyName string, region string) (bool, error) {
	var data struct {
		KeyPairs []struct {
			KeyName string
		}
	}
	args := []string{
		"ec2", "describe-key-pairs",
		"--region", region,
	}
	err := runJSONCommand(args, &data)
	if err != nil {
		return false, err
	}
	for _, keyPair := range data.KeyPairs {
		if keyPair.KeyName == keyName {
			return true, nil
		}
	}
	return false, nil
}

// sshKeyImport takes the user's local, public SSH key and imports it into the ec2 region so that
// we can create new hosts with it.
func sshKeyImport(keyName string, region string) error {
	keyBytes, err := ioutil.ReadFile(os.ExpandEnv(sshPublicKeyFile))
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrapf(err, "please run ssh-keygen externally to create your %s file", sshPublicKeyFile)
		}
		return err
	}

	var data struct {
		KeyName string
	}
	args := []string{
		"ec2", "import-key-pair",
		"--region", region,
		"--key-name", keyName,
		"--public-key-material", string(keyBytes),
	}
	return runJSONCommand(args, &data)
}

// sshKeyName computes the name of the ec2 ssh key that we'll store the local user's public key in
func (p *Provider) sshKeyName() (string, error) {
	user, err := p.FindActiveAccount()
	if err != nil {
		return "", err
	}

	keyBytes, err := ioutil.ReadFile(os.ExpandEnv(sshPublicKeyFile))
	if err != nil {
		if os.IsNotExist(err) {
			return "", errors.Wrapf(err, "please run ssh-keygen externally to create your %s file", sshPublicKeyFile)
		}
		return "", err
	}

	hash := sha1.New()
	if _, err := hash.Write(keyBytes); err != nil {
		return "", err
	}
	hashBytes := hash.Sum(make([]byte, 0, hash.BlockSize()))
	hashText := base64.URLEncoding.EncodeToString(hashBytes)

	return fmt.Sprintf("%s-%s", user, hashText), nil
}
