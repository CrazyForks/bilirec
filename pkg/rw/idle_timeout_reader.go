package rw

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

var ErrIdleTimeout = errors.New("read idle timeout")

// IdleTimeoutReadCloser wraps an io.ReadCloser and closes it from another
// goroutine when a single Read blocks longer than timeout.
type IdleTimeoutReadCloser struct {
	rc      io.ReadCloser
	timeout time.Duration
	fired   atomic.Bool
}

// NewIdleTimeoutReadCloser returns rc unchanged when timeout <= 0.
func NewIdleTimeoutReadCloser(rc io.ReadCloser, timeout time.Duration) io.ReadCloser {
	if rc == nil || timeout <= 0 {
		return rc
	}
	return &IdleTimeoutReadCloser{rc: rc, timeout: timeout}
}

func (r *IdleTimeoutReadCloser) Read(p []byte) (int, error) {
	n, err := r.readWithTimeout(p)
	if r.fired.Load() && err != nil {
		return n, fmt.Errorf("%w: %w", ErrIdleTimeout, err)
	}
	return n, err
}

func (r *IdleTimeoutReadCloser) Close() error {
	return r.rc.Close()
}

func (r *IdleTimeoutReadCloser) readWithTimeout(p []byte) (int, error) {
	timer := time.AfterFunc(r.timeout, r.fire)
	n, err := r.rc.Read(p)
	if !timer.Stop() {
		r.fired.Store(true)
	}
	return n, err
}

func (r *IdleTimeoutReadCloser) fire() {
	if !r.fired.CompareAndSwap(false, true) {
		return
	}
	_ = r.rc.Close()
}
