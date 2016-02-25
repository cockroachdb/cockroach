// +build windows

package readline

func init() {
	isWindows = true
}

// get width of the terminal
func getWidth(fd int) int {
	info, _ := GetConsoleScreenBufferInfo()
	if info == nil {
		return -1
	}
	return int(info.dwSize.x)
}
