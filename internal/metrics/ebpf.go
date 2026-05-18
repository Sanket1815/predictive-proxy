//go:build linux

package metrics

import (
	bpfpkg "github.com/nadsanket7/go-predictive-proxy/ebpf"
	"go.uber.org/zap"
)

type EBPFTracker = bpfpkg.EBPFTracker

func NewEBPFTracker(ifaceName string, log *zap.Logger) (*EBPFTracker, error) {
	return bpfpkg.NewEBPFTracker(ifaceName, log)
}
