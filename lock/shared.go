package lock

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	// bytes for two uint64 fields
	mmapSize = 8 * 2
)

// SharedToken manages permits stored in a memory-mapped file.
// Path must be accessible by both processes.
type SharedToken struct {
	maxWeight uint64
	f         *os.File
	mem       []byte
	// convenience pointers (point into mem)
	lastResetPtr  *uint64 // minute index
	permWeightPtr *uint64
	procMu        sync.Mutex // mutext to control access within a single process
}

// OpenSharedToken opens or creates the backing file and mmaps it.
// If the file is new it will be resized to mmapSize and initialized to zeros.
func OpenSharedToken(ctx context.Context, path string, maxWeight uint64, resetDuration time.Duration) (*SharedToken, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	// Ensure file size
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < mmapSize {
		if err := f.Truncate(mmapSize); err != nil {
			f.Close()
			return nil, err
		}
		// zeroing is automatic via truncate on most systems, but ensure bytes are zero:
		zero := make([]byte, mmapSize)
		if _, err := f.WriteAt(zero, 0); err != nil {
			f.Close()
			return nil, err
		}
	}

	// mmap the file (shared)
	b, err := unix.Mmap(int(f.Fd()), 0, mmapSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		f.Close()
		return nil, err
	}
	if len(b) < mmapSize {
		unix.Munmap(b)
		f.Close()
		return nil, errors.New("mmap returned too small region")
	}

	// pointers to the three uint64 fields (little-endian storage)
	// We use unsafe.Pointer to map underlying bytes to uint64 pointers.
	lastResetPtr := (*uint64)(unsafe.Pointer(&b[0]))
	permWeightPtr := (*uint64)(unsafe.Pointer(&b[8]))

	sp := &SharedToken{
		maxWeight:     maxWeight,
		f:             f,
		mem:           b,
		lastResetPtr:  lastResetPtr,
		permWeightPtr: permWeightPtr,
	}

	go func(ctx context.Context, sp *SharedToken, resetDuration time.Duration) {
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-time.After(max(time.Until(time.Now().Truncate(resetDuration).Add(resetDuration)), 0)):
				didReset, err := sp.ResetIfNeeded()
				if err != nil {
					logrus.Warnln("Shared Permits reset error:", err)
				} else {
					logrus.Infoln("Shared Permits reset:", t, didReset)
				}
			}
		}
	}(ctx, sp, resetDuration)
	return sp, nil
}

// Close unmaps and closes the backing file.
func (s *SharedToken) Close() error {
	if s.mem != nil {
		_ = unix.Munmap(s.mem)
		s.mem = nil
	}
	if s.f != nil {
		err := s.f.Close()
		s.f = nil
		return err
	}
	return nil
}

// lock obtains an exclusive flock on the backing file (blocks).
func (s *SharedToken) lock() error {
	return unix.Flock(int(s.f.Fd()), unix.LOCK_EX)
}

// unlock releases the flock.
func (s *SharedToken) unlock() error {
	return unix.Flock(int(s.f.Fd()), unix.LOCK_UN)
}

// helper to read little-endian uint64 from mem at offset (used only when not using atomic)
func (s *SharedToken) readAtOffset(off int) uint64 {
	return binary.LittleEndian.Uint64(s.mem[off : off+8])
}

// helper to write little-endian uint64 into mem at offset (used only when holding lock)
func (s *SharedToken) writeAtOffset(off int, v uint64) {
	binary.LittleEndian.PutUint64(s.mem[off:off+8], v)
}

// Read returns a snapshot of lastResetMinute, permitsWeight.
// This uses atomics for the numeric values so it's safe to call concurrently.
func (s *SharedToken) Read() (lastResetMinute uint64, permitsWeight uint64) {
	// lastReset may be updated under lock infrequently; do an atomic load of each field.
	// We cast to uintptr to avoid compiler escaping issues and use atomic.LoadUint64.
	lastResetMinute = atomic.LoadUint64(s.lastResetPtr)
	permitsWeight = atomic.LoadUint64(s.permWeightPtr)
	return
}

// ResetIfNeeded resets permits to fullness if currentMinute != lastResetMinute.
// maxRaw and maxWeight are the full minute allowances (e.g. 61000 and 6000).
// This acquires the flock to ensure only one process performs the reset.
func (s *SharedToken) ResetIfNeeded() (didReset bool, err error) {
	nowMin := uint64(time.Now().Unix() / 60)
	// fast path: read without lock
	last := atomic.LoadUint64(s.lastResetPtr)
	if last == nowMin {
		return false, nil // nothing to do
	}

	// serialize goroutines in this process
	s.procMu.Lock()
	defer s.procMu.Unlock()

	// acquire lock and re-check
	if err := s.lock(); err != nil {
		return false, err
	}
	defer s.unlock()

	// re-check under lock
	last = s.readAtOffset(0)
	if last == nowMin {
		return false, nil
	}

	// perform reset: set lastReset to nowMin and set permits to max values
	// write directly into memory (we hold lock), then ensure atomics will see it
	s.writeAtOffset(0, nowMin)
	s.writeAtOffset(8, s.maxWeight)

	// For readers that rely on atomic loads, store via atomic.StoreUint64 as well to ensure ordering.
	atomic.StoreUint64(s.lastResetPtr, nowMin)
	atomic.StoreUint64(s.permWeightPtr, s.maxWeight)

	return true, nil
}

// TryConsume tries to consume the requested raw and weight from shared permits.
// If there are not enough permits it returns (false, nil) and does not modify state.
// If it returns (true, nil) the permits have been decreased atomically (under lock).
// Use ResetIfNeeded before calling TryConsume periodically (or TryConsume will call it).
func (s *SharedToken) TryConsume(why string, weightReq uint64) (bool, error) {
	// We still need to serialize goroutines, otherwise two goroutines in-process could both pass the optimistic check.
	s.procMu.Lock()
	defer s.procMu.Unlock()

	// quick optimistic check without lock
	// but we must perform actual decrement under lock to be atomic
	nowMin := uint64(time.Now().Unix() / 60)

	// Acquire lock for the critical modify-check-modify operation
	if err := s.lock(); err != nil {
		return false, err
	}
	defer s.unlock()

	// Ensure state is fresh (reset if needed) while we hold lock
	last := s.readAtOffset(0)
	if last != nowMin {
		// reset
		s.writeAtOffset(0, nowMin)
		s.writeAtOffset(8, s.maxWeight)

		atomic.StoreUint64(s.lastResetPtr, nowMin)
		atomic.StoreUint64(s.permWeightPtr, s.maxWeight)
	}

	curWeight := s.readAtOffset(8)
	// fmt.Println(why, "Weight", curWeight)

	// check availability
	if weightReq > curWeight {
		// not enough: do not touch state
		return false, nil
	}

	// consume
	newWeight := curWeight - weightReq

	// write back
	s.writeAtOffset(8, newWeight)

	// update atomic views for readers
	atomic.StoreUint64(s.permWeightPtr, newWeight)

	return true, nil
}

func (s *SharedToken) Check(weightReq uint64) (bool, error) {
	// We still need to serialize goroutines, otherwise two goroutines in-process could both pass the optimistic check.
	s.procMu.Lock()
	defer s.procMu.Unlock()

	// quick optimistic check without lock
	// but we must perform actual decrement under lock to be atomic
	nowMin := uint64(time.Now().Unix() / 60)

	// Acquire lock for the critical modify-check-modify operation
	if err := s.lock(); err != nil {
		return false, err
	}
	defer s.unlock()

	// Ensure state is fresh (reset if needed) while we hold lock
	last := s.readAtOffset(0)
	if last != nowMin {
		// reset
		s.writeAtOffset(0, nowMin)
		s.writeAtOffset(8, s.maxWeight)

		atomic.StoreUint64(s.lastResetPtr, nowMin)
		atomic.StoreUint64(s.permWeightPtr, s.maxWeight)
	}

	curWeight := s.readAtOffset(8)

	// check availability
	return weightReq > curWeight, nil
}
