package nodeconn

import (
	"github.com/iotaledger/hive.go/app"
)

type ParametersNodeCon struct {
	GrpcURL               string `default:"grpc://localhost:50051" usage:"the gRPC address of the L1 IOTA node (grpc://host:port for plaintext, grpcs://host:port for TLS)"`
	PackageID             string `default:"" usage:"the identifier of the isc move package"`
	MaxConnectionAttempts uint   `default:"30" usage:"the amount of times the connection to INX will be attempted before it fails (1 attempt per second)"`
	TargetNetworkName     string `default:"" usage:"the network name on which the node should operate on (optional)"`
}

var ParamsL1 = &ParametersNodeCon{}

var params = &app.ComponentParams{
	Params: map[string]any{
		"l1": ParamsL1,
	},
	Masked: nil,
}
