package docker

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const FifoFile = "new_fifo"
const buildDockerContainerTimeout = 300

// loadDockerImage loads the docker image from a tar, and returns the image tag and an error.
func loadDockerImage(tarFilePath string) (string, error) {
	output, err := exec.Command(
		"docker",
		"load",
		"<",
		tarFilePath,
	).Output()
	if err != nil {
		return "", fmt.Errorf("cannot load docker image from %s", tarFilePath)
	}
	// The output will be "Loaded image: cockroachdb/cockroach-demo:latest".
	tokens := strings.Fields(string(output))
	// Extract the image name, e.g. "cockroachdb/cockroach-demo:latest".
	imageName := tokens[len(tokens)-1]
	return imageName, nil
}

// buildDockerContainer builds the docker container with containerName from a given
// docker image specified in dockerRunArgs.
func buildDockerContainer(dockerRunArgs []string, containerName string) error {
	// Must make sure that the container name is consistent with the one
	// in dockerRunArgs.
	dockerRunArgs = append(dockerRunArgs, fmt.Sprintf("--listening-url-file=%s", FifoFile))

	if len(dockerRunArgs) == 0 {
		return fmt.Errorf("args for docker run cannot be empty")
	}

	if dockerRunArgs[0] != "run" {
		return fmt.Errorf("the first arg of dockerRunArgs must be \"run\"")
	}

	output, err := exec.Command(
		"docker",
		dockerRunArgs...,
	).Output()
	if err != nil {
		return err
	}

	fmt.Printf("docker container is initialized with id %s, "+
		"not necessarily successfully built", string(output))

	// Check if the docker container is started by checking if the fifo is passed
	// with the url.
	finished := make(chan bool)
	go func() {
		output, err = exec.Command(
			"docker",
			"exec",
			"-i",
			containerName,
			"cat",
			FifoFile,
		).Output()
		finished <- true
	}()
	timeout := time.Second * buildDockerContainerTimeout
	select {
	case <-time.After(timeout):
		fmt.Println("timed out for building container")
		return nil
	case <-finished:
		if err != nil {
			return fmt.Errorf("failed reading fifo file: %v", err)
		}
		fmt.Printf("successfully built container, "+
			"with cockroach single node running on %s", string(output))
		return nil
	}
}

// executeSqlQuery executes the sql query in a given container,
// and returns the output of this query and an error.
func executeSqlQuery(
	query string, containerName string, cockroachSqlArgs []string,
) (string, error) {
	dockerExecCommand := append([]string{
		"exec",
		"-i",
		containerName,
		"./cockroach",
		"sql",
		"-e",
		fmt.Sprintf("%s;", query),
	}, cockroachSqlArgs...)

	cmd := exec.Command(
		"docker",
		dockerExecCommand...,
	)
	stdout, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("cannot execute sql query %s in docker container %s: %v", query, containerName, err)
	}
	// Return the output of sql query.
	return string(stdout), nil
}

// removeContainer removes a container,
// and returns an error if it cannot be removed or the removal times out.
func removeContainer(containerName string) error {
	cmd := exec.Command(
		"docker",
		"rm",
		"-f",
		containerName,
	)
	_, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("cannot remove a container %s: %v",
			containerName,
			err)
	}

	var output []byte
	timeout := time.Second * 300
	for start := time.Now(); ; {
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout for removing container %s", containerName)
		}
		output, err = exec.Command(
			"docker",
			"container",
			"ls",
			"-a",
			"|",
			"grep",
			containerName,
		).Output()
		if len(output) == 0 {
			fmt.Printf("docker container %s is fully stopped", containerName)
			break
		}
	}
	return nil
}
