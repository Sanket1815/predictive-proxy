//go:build linux

package metrics

import (
	bpfpkg "github.com/nadsanket7/go-predictive-proxy/ebpf"
	"go.uber.org/zap"
)

// EBPFTracker aliases the implementation in the ebpf package.
// The real struct and its usage of bpf2go-generated unexported types
// (bpfObjects, loadBpfObjects) must live in package ebpf where those
// types are in scope.
type EBPFTracker = bpfpkg.EBPFTracker

// NewEBPFTracker delegates to the ebpf package implementation.
func NewEBPFTracker(ifaceName string, log *zap.Logger) (*EBPFTracker, error) {
	return bpfpkg.NewEBPFTracker(ifaceName, log)
}
