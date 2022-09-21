package logictest

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
)

func getUsedPorts() (map[int]struct{}, error) {
	portsFilePath := bazel.TestTmpDir() + "/ports.txt"
	fmt.Println(portsFilePath)
	f, err := os.OpenFile(portsFilePath, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	ports := make(map[int]struct{})

	scanner := bufio.NewScanner(f)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	for scanner.Scan() {
		port, err := strconv.Atoi(scanner.Text())
		fmt.Println("port:", port)
		if err != nil {
			return nil, err
		}
		ports[port] = struct{}{}
	}
	return ports, nil
}

// Grab a port and write to temporary text file so we don't re-use
// the ports, in another instance of testserver. Even if we kill a node,
// we need to make sure that port is not taken since the node may
// be restarted.
func getFreePort() (int, error) {
	for {
		addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
		if err != nil {
			return 0, err
		}

		l, err := net.ListenTCP("tcp", addr)
		if err != nil {
			return 0, err
		}
		port := l.Addr().(*net.TCPAddr).Port
		usedPorts, err := getUsedPorts()
		if err != nil {
			return 0, err
		}
		if _, found := usedPorts[port]; found {
			continue
		}

		portsFilePath := bazel.TestTmpDir() + "/ports.txt"
		fmt.Println(portsFilePath)
		f, err := os.OpenFile(portsFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return 0, err
		}

		_, err = f.WriteString(fmt.Sprintf("%d\n", port))

		err = l.Close()
		if err != nil {
			return 0, err
		}

		return port, err
	}
}
