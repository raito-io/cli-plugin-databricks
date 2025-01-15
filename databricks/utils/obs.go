package utils

import (
	"fmt"
	"runtime"
)

func MemoryUsage(print func(msg string, args ...interface{})) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	print(fmt.Sprintf("Heap: %v MiB; System memory: %v MiB", bToMb(m.Alloc), bToMb(m.Sys)))
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
