// Package csi implements a CSI node plugin that mounts the jellyfin-cache
// daemon into pods using the 9P2000.L protocol. No controller is needed
// because only ephemeral inline volumes are supported.
package csi

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
)

// DriverName is the CSI driver identifier registered in Kubernetes.
const DriverName = "jellyfin-cache.csi.io"

// Server wraps the gRPC server for the CSI Identity and Node services.
type Server struct {
	nodeID   string
	endpoint string
	version  string
	log      *slog.Logger
}

// NewServer creates a Server.
func NewServer(nodeID, endpoint, version string, log *slog.Logger) *Server {
	return &Server{nodeID: nodeID, endpoint: endpoint, version: version, log: log}
}

// Run starts the gRPC server and blocks until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	scheme, addr, err := parseEndpoint(s.endpoint)
	if err != nil {
		return err
	}
	if scheme == "unix" {
		if err := os.Remove(addr); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing stale CSI socket %q: %w", addr, err)
		}
	}

	listener, err := net.Listen(scheme, addr)
	if err != nil {
		return fmt.Errorf("CSI listen on %q: %w", s.endpoint, err)
	}
	s.log.Info("CSI node plugin listening", "endpoint", s.endpoint, "node", s.nodeID)

	srv := grpc.NewServer()
	csipb.RegisterIdentityServer(srv, &identityServer{version: s.version})
	csipb.RegisterNodeServer(srv, &nodeServer{nodeID: s.nodeID, log: s.log})

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	return srv.Serve(listener)
}

// parseEndpoint splits "unix:///path" or "tcp://host:port" into (scheme, addr).
func parseEndpoint(ep string) (string, string, error) {
	switch {
	case strings.HasPrefix(ep, "unix://"):
		return "unix", strings.TrimPrefix(ep, "unix://"), nil
	case strings.HasPrefix(ep, "tcp://"):
		return "tcp", strings.TrimPrefix(ep, "tcp://"), nil
	default:
		return "", "", fmt.Errorf("unsupported CSI endpoint %q (use unix:// or tcp://)", ep)
	}
}
