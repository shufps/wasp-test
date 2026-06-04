package iscmoveclienttest

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/iotaledger/wasp/clients/iota-go/iotaclient"
	"github.com/iotaledger/wasp/clients/iota-go/iotagrpc"
	"github.com/iotaledger/wasp/clients/iscmove/iscmoveclient"
	"github.com/iotaledger/wasp/packages/cryptolib"
	"github.com/iotaledger/wasp/packages/testutil/l1starter"
)

func NewSignerWithFunds(t *testing.T, seed []byte, index int) cryptolib.Signer {
	seed[0] += byte(index)
	kp := cryptolib.KeyPairFromSeed(cryptolib.Seed(seed))
	err := iotaclient.RequestFundsFromFaucet(context.Background(), kp.Address().AsIotaAddress(), l1starter.Instance().FaucetURL())
	require.NoError(t, err)
	return kp
}

func NewRandomSignerWithFunds(t *testing.T, index int) cryptolib.Signer {
	seed := cryptolib.NewSeed()
	return NewSignerWithFunds(t, seed[:], index)
}

func NewHTTPClient() *iscmoveclient.SoloClient {
	httpClient := iscmoveclient.NewHTTPClient(
		l1starter.Instance().APIURL(),
		l1starter.Instance().FaucetURL(),
		l1starter.WaitUntilEffectsVisible,
	)
	var grpcClient *iotagrpc.Client
	if grpcURL := l1starter.Instance().GrpcURL(); grpcURL != "" {
		var err error
		grpcClient, err = iotagrpc.NewClient(grpcURL)
		if err != nil {
			panic(fmt.Sprintf("iscmoveclienttest.NewHTTPClient: gRPC client: %v", err))
		}
	}
	return iscmoveclient.NewSoloClient(httpClient, grpcClient)
}
