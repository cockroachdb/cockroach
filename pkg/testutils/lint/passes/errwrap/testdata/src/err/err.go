package err

import "errors"

func singleReturn(err error) error {
	if err == nil {
		err = errors.New("foo")
	}
	return err
}

func multiReturn(str string, err error, int int) (string, error, int) {
	if err == nil {
		str = "a"
		err = errors.New("bar")
		int = 10
	}
	return str, err, int
}

func multiCallReturn() (string, error, int) {
	err := errors.New("baz")
	return multiReturn("abc", err, 20)
}

func singleCallReturn() error {
	err := errors.New("bat")
	return singleReturn(err)
}
