package util

import (
	"hash/crc32"
)

// CRC32 computes the CRC32 checksum of data.
// Used for detecting corruption in WAL records and SSTable sections.
func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// VerifyCRC32 checks if the given checksum matches the computed checksum.
// Returns true if the data is not corrupted.
func VerifyCRC32(data []byte, expectedCRC uint32) bool {
	return CRC32(data) == expectedCRC
}
