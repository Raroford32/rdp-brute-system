// +build linux

package worker

import (
	"fmt"
	"runtime"
	"syscall"
	"unsafe"
	"rdp-brute-system/shared/logger"
)

// CPUSet represents a CPU affinity mask
type CPUSet struct {
	bits [16]uint64 // Support up to 1024 CPUs
}

// Set sets the CPU bit
func (c *CPUSet) Set(cpu int) {
	c.bits[cpu/64] |= 1 << (cpu % 64)
}

// Clear clears the CPU bit
func (c *CPUSet) Clear(cpu int) {
	c.bits[cpu/64] &^= 1 << (cpu % 64)
}

// IsSet checks if CPU bit is set
func (c *CPUSet) IsSet(cpu int) bool {
	return c.bits[cpu/64]&(1<<(cpu%64)) != 0
}

// setCPUAffinity sets CPU affinity for the worker on Linux
func (w *Worker) setCPUAffinity() {
	numCPU := runtime.NumCPU()
	
	// Distribute workers across CPU cores
	// Use worker ID to determine which CPUs to bind to
	startCPU := w.id % numCPU
	
	// Create CPU set
	var cpuSet CPUSet
	
	// Bind to 2-4 CPUs depending on availability
	cpusToUse := 2
	if numCPU >= 8 {
		cpusToUse = 4
	} else if numCPU >= 4 {
		cpusToUse = 2
	} else {
		cpusToUse = 1
	}
	
	// Set CPU affinity mask
	for i := 0; i < cpusToUse; i++ {
		cpu := (startCPU + i) % numCPU
		cpuSet.Set(cpu)
	}
	
	// Apply CPU affinity to current thread
	tid := syscall.Gettid()
	_, _, errno := syscall.Syscall(syscall.SYS_SCHED_SETAFFINITY,
		uintptr(tid),
		unsafe.Sizeof(cpuSet),
		uintptr(unsafe.Pointer(&cpuSet)))
	
	if errno != 0 {
		logger.WorkerLogger.Error("Failed to set CPU affinity for worker", map[string]interface{}{
			"worker_id": w.id,
			"error":     errno,
		})
		return
	}
	
	// Get affinity to verify
	var getCpuSet CPUSet
	_, _, errno = syscall.Syscall(syscall.SYS_SCHED_GETAFFINITY,
		uintptr(tid),
		unsafe.Sizeof(getCpuSet),
		uintptr(unsafe.Pointer(&getCpuSet)))
	
	if errno == 0 {
		cpuList := ""
		for i := 0; i < numCPU; i++ {
			if getCpuSet.IsSet(i) {
				if cpuList != "" {
					cpuList += ","
				}
				cpuList += fmt.Sprintf("%d", i)
			}
		}
		logger.WorkerLogger.Info("Worker CPU affinity set", map[string]interface{}{
			"worker_id": w.id,
			"cpus":      cpuList,
		})
	}
}

// SetWorkerThreadAffinity sets CPU affinity for a specific worker thread
func SetWorkerThreadAffinity(workerID, threadID int) {
	numCPU := runtime.NumCPU()
	
	// Calculate which CPU this thread should run on
	cpu := (workerID + threadID) % numCPU
	
	var cpuSet CPUSet
	cpuSet.Set(cpu)
	
	tid := syscall.Gettid()
	_, _, errno := syscall.Syscall(syscall.SYS_SCHED_SETAFFINITY,
		uintptr(tid),
		unsafe.Sizeof(cpuSet),
		uintptr(unsafe.Pointer(&cpuSet)))
	
	if errno != 0 {
		// Silent fail - CPU affinity is optimization, not critical
		return
	}
}
