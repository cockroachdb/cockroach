package sqlsmith

import "math/rand"

func coin() bool {
	return rand.Intn(2) == 0
}

func d6() int {
	return rand.Intn(6) + 1
}

func d9() int {
	return rand.Intn(6) + 1
}

func d20() int {
	return rand.Intn(20) + 1
}

func d42() int {
	return rand.Intn(42) + 1
}

func d100() int {
	return rand.Intn(100) + 1
}
