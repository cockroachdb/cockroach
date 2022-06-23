package hdrhistogram

import "fmt"

const truncatedErrStr = "Truncated compressed histogram decode. Expected minimum length of %d bytes and got %d."

// Read an LEB128 ZigZag encoded long value from the given buffer
func zig_zag_decode_i64(buf []byte) (signedValue int64, n int, err error) {
	buflen := len(buf)
	if buflen < 1 {
		return 0, 0, nil
	}
	var value = uint64(buf[0]) & 0x7f
	n = 1
	if (buf[0] & 0x80) != 0 {
		if buflen < 2 {
			err = fmt.Errorf(truncatedErrStr, 2, buflen)
			return
		}
		value |= uint64(buf[1]) & 0x7f << 7
		n = 2
		if (buf[1] & 0x80) != 0 {
			if buflen < 3 {
				err = fmt.Errorf(truncatedErrStr, 3, buflen)
				return
			}
			value |= uint64(buf[2]) & 0x7f << 14
			n = 3
			if (buf[2] & 0x80) != 0 {
				if buflen < 4 {
					err = fmt.Errorf(truncatedErrStr, 4, buflen)
					return
				}
				value |= uint64(buf[3]) & 0x7f << 21
				n = 4
				if (buf[3] & 0x80) != 0 {
					if buflen < 5 {
						err = fmt.Errorf(truncatedErrStr, 5, buflen)
						return
					}
					value |= uint64(buf[4]) & 0x7f << 28
					n = 5
					if (buf[4] & 0x80) != 0 {
						if buflen < 6 {
							err = fmt.Errorf(truncatedErrStr, 6, buflen)
							return
						}
						value |= uint64(buf[5]) & 0x7f << 35
						n = 6
						if (buf[5] & 0x80) != 0 {
							if buflen < 7 {
								err = fmt.Errorf(truncatedErrStr, 7, buflen)
								return
							}
							value |= uint64(buf[6]) & 0x7f << 42
							n = 7
							if (buf[6] & 0x80) != 0 {
								if buflen < 8 {
									err = fmt.Errorf(truncatedErrStr, 8, buflen)
									return
								}
								value |= uint64(buf[7]) & 0x7f << 49
								n = 8
								if (buf[7] & 0x80) != 0 {
									if buflen < 9 {
										err = fmt.Errorf(truncatedErrStr, 9, buflen)
										return
									}
									value |= uint64(buf[8]) << 56
									n = 9
								}
							}
						}
					}
				}
			}
		}
	}
	signedValue = int64((value >> 1) ^ -(value & 1))
	return
}

// Writes a int64_t value to the given buffer in LEB128 ZigZag encoded format
// ZigZag encoding maps signed integers to unsigned integers so that numbers with a small
// absolute value (for instance, -1) have a small varint encoded value too.
// It does this in a way that "zig-zags" back and forth through the positive and negative integers,
// so that -1 is encoded as 1, 1 is encoded as 2, -2 is encoded as 3, and so on.
func zig_zag_encode_i64(signedValue int64) (buffer []byte) {
	buffer = make([]byte, 0)
	var value = uint64((signedValue << 1) ^ (signedValue >> 63))
	if value>>7 == 0 {
		buffer = append(buffer, byte(value))
	} else {
		buffer = append(buffer, byte((value&0x7F)|0x80))
		if value>>14 == 0 {
			buffer = append(buffer, byte(value>>7))
		} else {
			buffer = append(buffer, byte((value>>7)|0x80))
			if value>>21 == 0 {
				buffer = append(buffer, byte(value>>14))
			} else {
				buffer = append(buffer, byte((value>>14)|0x80))
				if value>>28 == 0 {
					buffer = append(buffer, byte(value>>21))
				} else {
					buffer = append(buffer, byte((value>>21)|0x80))
					if value>>35 == 0 {
						buffer = append(buffer, byte(value>>28))
					} else {
						buffer = append(buffer, byte((value>>28)|0x80))
						if value>>42 == 0 {
							buffer = append(buffer, byte(value>>35))
						} else {
							buffer = append(buffer, byte((value>>35)|0x80))
							if value>>49 == 0 {
								buffer = append(buffer, byte(value>>42))
							} else {
								buffer = append(buffer, byte((value>>42)|0x80))
								if value>>56 == 0 {
									buffer = append(buffer, byte(value>>49))
								} else {
									buffer = append(buffer, byte((value>>49)|0x80))
									buffer = append(buffer, byte(value>>56))
								}
							}
						}
					}
				}
			}
		}
	}
	return
}
