package observability

import "os"

// logOutput returns the writer used by NewLogger.
// Separated so tests can substitute it without touching the build tag machinery.
func logOutput() *os.File { return os.Stdout }
