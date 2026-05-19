package l1starter

import (
	"context"
	"fmt"
	"strings"

	"github.com/iotaledger/wasp/clients"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotasigner"
	"github.com/iotaledger/wasp/packages/cryptolib"
)

type RemoteIotaNode struct {
	faucetURL       string
	apiURL          string
	grpcURL         string // if empty, derived from apiURL
	iscPackageOwner iotasigner.Signer
	iscPackageID    iotago.PackageID
}

func NewRemoteIotaNode(apiURL string, faucetURL string, iscPackageOwner iotasigner.Signer) *RemoteIotaNode {
	return &RemoteIotaNode{
		faucetURL:       faucetURL,
		apiURL:          apiURL,
		iscPackageOwner: iscPackageOwner,
	}
}

func NewRemoteIotaNodeWithGrpc(apiURL, faucetURL, grpcURL string, iscPackageOwner iotasigner.Signer) *RemoteIotaNode {
	return &RemoteIotaNode{
		faucetURL:       faucetURL,
		apiURL:          apiURL,
		grpcURL:         grpcURL,
		iscPackageOwner: iscPackageOwner,
	}
}

func (r *RemoteIotaNode) ISCPackageID() iotago.PackageID {
	return r.iscPackageID
}

func (r *RemoteIotaNode) APIURL() string {
	return r.apiURL
}

func (r *RemoteIotaNode) FaucetURL() string {
	return r.faucetURL
}

func (r *RemoteIotaNode) GrpcURL() string {
	if r.grpcURL != "" {
		return r.grpcURL
	}
	// Derive from API URL: replace http(s):// with grpc://
	u := r.apiURL
	if strings.HasPrefix(u, "https://") {
		u = "grpc://" + u[len("https://"):]
	} else if strings.HasPrefix(u, "http://") {
		u = "grpc://" + u[len("http://"):]
	}
	return u
}

func (r *RemoteIotaNode) L1Client() clients.L1Client {
	return clients.NewL1Client(clients.L1Config{
		APIURL:    r.APIURL(),
		FaucetURL: r.FaucetURL(),
	}, WaitUntilEffectsVisible)
}

func (r *RemoteIotaNode) IsLocal() bool {
	return false
}

func (r *RemoteIotaNode) start(ctx context.Context) {
	client := r.L1Client()

	err := client.RequestFunds(ctx, *cryptolib.NewAddressFromIota(r.iscPackageOwner.Address()))
	if err != nil {
		panic(fmt.Errorf("faucet request failed: %w for url: %s", err, r.faucetURL))
	}

	r.iscPackageID, err = client.DeployISCContracts(ctx, r.iscPackageOwner)
	if err != nil {
		panic(fmt.Errorf("isc contract deployment failed: %w", err))
	}
}
