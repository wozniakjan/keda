//go:build e2e
// +build e2e

// Package require is a thin shim around github.com/stretchr/testify/require
// that adds a global OnFailure hook fired the moment a required assertion
// fails — before testify calls t.FailNow() and before any test defers or
// t.Cleanup functions run.
//
// Usage: replace `"github.com/stretchr/testify/require"` with
// `"github.com/kedacore/keda/v2/tests/helper/require"` in e2e test files.
// The public function signatures are identical, so no call-site changes are
// needed.
//
// Register the hook once per test binary (e.g. in TestMain or a setup
// helper). The hook receives the TestingT and the formatted failure message
// from testify. It runs synchronously: keep it fast or it will block FailNow.
//
//	require.OnFailure = func(t require.TestingT, msg string) {
//	    helper.DumpClusterState(t, msg)
//	}
package require

import (
	"fmt"
	"sync"

	tfyrequire "github.com/stretchr/testify/require"
)

// TestingT mirrors testify's required surface so we can wrap any value that
// satisfies it (typically *testing.T).
type TestingT interface {
	Errorf(format string, args ...interface{})
	FailNow()
}

// FailureHook receives the wrapped TestingT and the formatted failure
// message produced by testify. It is invoked before t.FailNow() is called,
// so the cluster state at the moment of failure is still intact.
type FailureHook func(t TestingT, msg string)

var (
	hookMu     sync.RWMutex
	onFailure  FailureHook
	hookActive sync.Map // tracks (t -> bool) so a single test fires the hook at most once
)

// SetOnFailure registers (or clears, when fn is nil) the global failure hook.
// Safe to call concurrently.
func SetOnFailure(fn FailureHook) {
	hookMu.Lock()
	defer hookMu.Unlock()
	onFailure = fn
}

// hookedT intercepts Errorf calls from testify and invokes the registered
// hook before delegating to the underlying TestingT.
type hookedT struct {
	TestingT
}

func (h *hookedT) Errorf(format string, args ...interface{}) {
	hookMu.RLock()
	fn := onFailure
	hookMu.RUnlock()
	if fn != nil {
		if _, already := hookActive.LoadOrStore(h.TestingT, true); !already {
			fn(h.TestingT, fmt.Sprintf(format, args...))
		}
	}
	h.TestingT.Errorf(format, args...)
}

// Helper forwards t.Helper() so test output still points at the caller line.
func (h *hookedT) Helper() {
	if helper, ok := h.TestingT.(interface{ Helper() }); ok {
		helper.Helper()
	}
}

func wrap(t TestingT) *hookedT { return &hookedT{TestingT: t} }

// The wrappers below are 1:1 with testify's require functions. Add more here
// as the test code starts using them — `grep -rohE 'require\.[A-Z][A-Za-z]+'
// tests/` lists the current set.

// True asserts that the specified value is true.
func True(t TestingT, value bool, msgAndArgs ...interface{}) {
	tfyrequire.True(wrap(t), value, msgAndArgs...)
}

// Nil asserts that the specified object is nil.
func Nil(t TestingT, object interface{}, msgAndArgs ...interface{}) {
	tfyrequire.Nil(wrap(t), object, msgAndArgs...)
}

// NoError asserts that a function returned no error (i.e. nil).
func NoError(t TestingT, err error, msgAndArgs ...interface{}) {
	tfyrequire.NoError(wrap(t), err, msgAndArgs...)
}

// NoErrorf asserts that a function returned no error (i.e. nil).
func NoErrorf(t TestingT, err error, msg string, args ...interface{}) {
	tfyrequire.NoErrorf(wrap(t), err, msg, args...)
}

// NotEmpty asserts that the specified object is NOT empty.
func NotEmpty(t TestingT, object interface{}, msgAndArgs ...interface{}) {
	tfyrequire.NotEmpty(wrap(t), object, msgAndArgs...)
}
