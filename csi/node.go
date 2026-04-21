package csi

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeServer struct {
	csipb.UnimplementedNodeServer
	nodeID string
	log    *slog.Logger
}

// NodePublishVolume mounts the cache server at TargetPath.
//
// Required volumeAttributes:
//   - server: hostname or IP of the jellyfin-cache server
//
// Optional volumeAttributes:
//   - protocol: "nfs" (default) or "9p" — must match daemon's mount.type
//   - port:     TCP port; defaults to 2049 for nfs, 5640 for 9p
func (n *nodeServer) NodePublishVolume(_ context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}

	vc := req.GetVolumeContext()
	server := vc["server"]
	if server == "" {
		return nil, status.Error(codes.InvalidArgument, "volumeAttributes.server is required")
	}

	protocol := vc["protocol"]
	if protocol == "" {
		protocol = "nfs"
	}

	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, status.Errorf(codes.Internal, "creating target dir %q: %v", target, err)
	}

	if mounted, err := isMounted(target); err != nil {
		return nil, status.Errorf(codes.Internal, "checking mount state of %q: %v", target, err)
	} else if mounted {
		n.log.Info("already mounted, skipping", "target", target)
		return &csipb.NodePublishVolumeResponse{}, nil
	}

	n.log.Info("mounting", "protocol", protocol, "server", server, "target", target)

	var mountErr error
	switch protocol {
	case "nfs":
		mountErr = mountNFS(server, target, vc["port"], req.GetReadonly())
	case "9p":
		mountErr = mount9P(server, target, vc["port"], req.GetReadonly())
	default:
		return nil, status.Errorf(codes.InvalidArgument, "unknown protocol %q (use \"nfs\" or \"9p\")", protocol)
	}

	if mountErr != nil {
		return nil, status.Errorf(codes.Internal, "mount %s %s → %s: %v", protocol, server, target, mountErr)
	}
	return &csipb.NodePublishVolumeResponse{}, nil
}

func mountNFS(server, target, port string, readonly bool) error {
	if port == "" {
		port = "2049"
	}
	// The kernel NFS client requires addr= when using text-format mount options;
	// it cannot resolve DNS itself, which would cause EINVAL.
	addrs, err := net.LookupHost(server)
	if err != nil {
		return fmt.Errorf("resolving %q: %w", server, err)
	}
	opts := fmt.Sprintf("addr=%s,port=%s,mountport=%s,nfsvers=3,proto=tcp,mountproto=tcp,nolock", addrs[0], port, port)
	if readonly {
		opts += ",ro"
	}
	return syscall.Mount(server+":/", target, "nfs", 0, opts)
}

func mount9P(server, target, port string, readonly bool) error {
	if port == "" {
		port = "5640"
	}
	opts := fmt.Sprintf("trans=tcp,port=%s,version=9p2000.L", port)
	var flags uintptr
	if readonly {
		flags |= syscall.MS_RDONLY
	}
	return syscall.Mount(server, target, "9p", flags, opts)
}

// NodeUnpublishVolume unmounts TargetPath. Idempotent: returns success if not mounted.
func (n *nodeServer) NodeUnpublishVolume(_ context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	target := req.GetTargetPath()
	if target == "" {
		return nil, status.Error(codes.InvalidArgument, "target_path is required")
	}

	if mounted, err := isMounted(target); err != nil {
		return nil, status.Errorf(codes.Internal, "checking mount state of %q: %v", target, err)
	} else if !mounted {
		return &csipb.NodeUnpublishVolumeResponse{}, nil
	}

	n.log.Info("unmounting", "target", target)
	if err := syscall.Unmount(target, 0); err != nil {
		return nil, status.Errorf(codes.Internal, "unmount %q: %v", target, err)
	}

	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (n *nodeServer) NodeGetCapabilities(_ context.Context, _ *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{}, nil
}

func (n *nodeServer) NodeGetInfo(_ context.Context, _ *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	return &csipb.NodeGetInfoResponse{NodeId: n.nodeID}, nil
}

// isMounted reports whether target is listed in /proc/mounts.
func isMounted(target string) (bool, error) {
	f, err := os.Open("/proc/mounts")
	if err != nil {
		return false, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[1] == target {
			return true, nil
		}
	}
	return false, scanner.Err()
}
