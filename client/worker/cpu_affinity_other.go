// +build !linux

package worker

import (
	"log"
)

// setCPUAffinity is a no-op on non-Linux systems
func (w *Worker) setCPUAffinity() {
	log.Printf("CPU affinity not supported on this platform")
}

// SetWorkerThreadAffinity is a no-op on non-Linux systems
func SetWorkerThreadAffinity(workerID, threadID int) {
	// No-op on non-Linux systems
}