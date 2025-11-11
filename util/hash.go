package util

import "hash/fnv"

const hashMask = uint32(0x7fffffff)

// Hash returns a non-negative int hash of the given string key.
func Hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & hashMask)
}

// GenerateID returns a 64-bit FNV-1a hash of the given payload string.
func GenerateID(payload string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(payload))
	return h.Sum64()
}
