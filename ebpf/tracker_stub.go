//go:build !linux

// Package ebpf provides a no-op EBPFTracker stub for non-Linux platforms
// (Windows, macOS). The real implementation lives in tracker.go (linux only).
package ebpf

import "go.uber.org/zap"

// EBPFTracker is a no-op stub on non-Linux platforms.
type EBPFTracker struct{}

func NewEBPFTracker(_ string, _ *zap.Logger) (*EBPFTracker, error) {
	return &EBPFTracker{}, nil
}

func (t *EBPFTracker) IsSlow(_ [4]byte) bool { return false }
func (t *EBPFTracker) Close() error           { return nil }
