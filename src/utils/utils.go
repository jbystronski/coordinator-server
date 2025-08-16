package utils

import (
	"encoding/binary"
	"log"
	"os"
	"strconv"
)

func JoinStrings(arr []string, sep string) string {
	result := ""
	for i, s := range arr {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}

func U64b(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}

func GetIntEnv(name string, defaultVal int) int {
	res := defaultVal
	if val := os.Getenv(name); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			res = n
		} else {
			log.Printf("Invalid %v, using default: %d", name, res)
		}
	}
	return res
}
