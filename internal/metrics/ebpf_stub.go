//go:build !linux

package metrics

import (
	bpfpkg "github.com/nadsanket7/go-predictive-proxy/ebpf"
	"go.uber.org/zap"
)

// EBPFTracker aliases the no-op stub from the ebpf package.
type EBPFTracker = bpfpkg.EBPFTracker

// NewEBPFTracker returns a no-op stub on non-Linux platforms.
func NewEBPFTracker(_ string, _ *zap.Logger) (*EBPFTracker, error) {
	return bpfpkg.NewEBPFTracker("", nil)
}
