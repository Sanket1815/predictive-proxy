//go:build !linux

package ebpf

import "go.uber.org/zap"

type EBPFTracker struct{}

func NewEBPFTracker(_ string, _ *zap.Logger) (*EBPFTracker, error) {
	return &EBPFTracker{}, nil
}

func (t *EBPFTracker) IsSlow(_ [4]byte) bool { return false }
func (t *EBPFTracker) Close() error           { return nil }
