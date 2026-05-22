//go:build e2e
// +build e2e

package require_test

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	khelperrequire "github.com/kedacore/keda/v2/tests/helper/require"
)

// fakeT captures Errorf calls and signals FailNow without aborting the
// surrounding test goroutine.
type fakeT struct {
	mu     sync.Mutex
	errs   []string
	failed atomic.Bool
}

func (f *fakeT) Errorf(format string, args ...interface{}) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs = append(f.errs, format)
}

func (f *fakeT) FailNow() { f.failed.Store(true) }

func TestHookFiresOnFailure(t *testing.T) {
	t.Cleanup(func() { khelperrequire.SetOnFailure(nil) })

	var (
		fired atomic.Int32
		gotT  khelperrequire.TestingT
		gotMsg string
	)
	khelperrequire.SetOnFailure(func(tt khelperrequire.TestingT, msg string) {
		fired.Add(1)
		gotT = tt
		gotMsg = msg
	})

	ft := &fakeT{}
	khelperrequire.True(ft, false, "value should be true")

	if fired.Load() != 1 {
		t.Fatalf("expected hook to fire once, got %d", fired.Load())
	}
	if gotT != ft {
		t.Fatalf("hook received wrong TestingT")
	}
	if gotMsg == "" {
		t.Fatalf("hook received empty message")
	}
	if !ft.failed.Load() {
		t.Fatalf("expected FailNow to be called")
	}
}

func TestHookDoesNotFireOnSuccess(t *testing.T) {
	t.Cleanup(func() { khelperrequire.SetOnFailure(nil) })

	var fired atomic.Int32
	khelperrequire.SetOnFailure(func(_ khelperrequire.TestingT, _ string) {
		fired.Add(1)
	})

	ft := &fakeT{}
	khelperrequire.True(ft, true, "should pass")
	khelperrequire.NoError(ft, nil)
	khelperrequire.Nil(ft, nil)
	khelperrequire.NotEmpty(ft, "non-empty")

	if fired.Load() != 0 {
		t.Fatalf("expected hook to never fire, got %d", fired.Load())
	}
	if ft.failed.Load() {
		t.Fatalf("FailNow should not have been called")
	}
}

func TestHookFiresOnlyOncePerT(t *testing.T) {
	t.Cleanup(func() { khelperrequire.SetOnFailure(nil) })

	var fired atomic.Int32
	khelperrequire.SetOnFailure(func(_ khelperrequire.TestingT, _ string) {
		fired.Add(1)
	})

	ft := &fakeT{}
	khelperrequire.True(ft, false, "first fail")
	khelperrequire.NoError(ft, errors.New("second fail"))
	khelperrequire.Nil(ft, "not nil")

	if got := fired.Load(); got != 1 {
		t.Fatalf("expected hook to fire exactly once per TestingT, got %d", got)
	}
}

func TestNoHookRegisteredIsSafe(t *testing.T) {
	khelperrequire.SetOnFailure(nil)
	ft := &fakeT{}
	khelperrequire.True(ft, false, "no hook")
	if !ft.failed.Load() {
		t.Fatalf("expected FailNow even without a hook")
	}
}
