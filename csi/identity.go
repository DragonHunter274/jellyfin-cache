package csi

import (
	"context"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
)

type identityServer struct {
	csipb.UnimplementedIdentityServer
	version string
}

func (i *identityServer) GetPluginInfo(_ context.Context, _ *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          DriverName,
		VendorVersion: i.version,
	}, nil
}

func (i *identityServer) GetPluginCapabilities(_ context.Context, _ *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	// Node-only driver; no controller capabilities.
	return &csipb.GetPluginCapabilitiesResponse{}, nil
}

func (i *identityServer) Probe(_ context.Context, _ *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	return &csipb.ProbeResponse{}, nil
}
