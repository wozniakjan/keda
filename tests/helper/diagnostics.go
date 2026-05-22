//go:build e2e
// +build e2e

package helper

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	helperrequire "github.com/kedacore/keda/v2/tests/helper/require"
)

// DiagnosticsDir is the directory failure markers are written under. If
// E2E_DIAGNOSTICS_DIR is set it wins; otherwise GITHUB_WORKSPACE; otherwise
// the current working directory. The "_diagnostics" subdirectory is created
// on first use.
func DiagnosticsDir() string {
	if d := os.Getenv("E2E_DIAGNOSTICS_DIR"); d != "" {
		return d
	}
	if d := os.Getenv("GITHUB_WORKSPACE"); d != "" {
		return filepath.Join(d, "_diagnostics")
	}
	return "_diagnostics"
}

var nameSanitizer = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func sanitizeForFilename(s string) string {
	return strings.Trim(nameSanitizer.ReplaceAllString(s, "_"), "_")
}

// defaultFailureHookMu serializes hook invocations so concurrent test
// failures don't interleave file writes or compete for the apiserver. The
// hook is cheap; serialization is fine.
var defaultFailureHookMu sync.Mutex

// defaultFailureHook is the placeholder hook registered at init() time. It
// records a single .log file per failure with the test name, message and
// timestamp so we can confirm the wiring fires end-to-end in CI. A richer
// cluster-state dump (kubectl get/describe/logs across nodes, KEDA pods and
// e2e namespaces) belongs in a follow-up change — extend this function
// without touching the wiring.
func defaultFailureHook(t helperrequire.TestingT, msg string) {
	defaultFailureHookMu.Lock()
	defer defaultFailureHookMu.Unlock()

	name := "unknown"
	if named, ok := t.(interface{ Name() string }); ok {
		name = named.Name()
	}

	dir := DiagnosticsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "diagnostics: cannot create %s: %v\n", dir, err)
		return
	}

	ts := time.Now().UTC()
	fname := fmt.Sprintf("failure_%s_%d.log", sanitizeForFilename(name), ts.UnixNano())
	path := filepath.Join(dir, fname)

	body := fmt.Sprintf(
		"timestamp: %s\ntest: %s\nmessage: %s\n",
		ts.Format(time.RFC3339Nano), name, msg,
	)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "diagnostics: cannot write %s: %v\n", path, err)
		return
	}

	if logger, ok := t.(interface{ Logf(string, ...interface{}) }); ok {
		logger.Logf("diagnostics: failure marker written to %s", path)
	}
}

// init wires the default failure hook. Every e2e test binary imports
// tests/helper, so this fires exactly once per binary at startup.
func init() {
	helperrequire.SetOnFailure(defaultFailureHook)
}

// Compile-time guard that *testing.T continues to satisfy the shim's
// TestingT contract — protects against accidental drift if the shim's
// interface is widened in the future.
var _ helperrequire.TestingT = (*testing.T)(nil)
