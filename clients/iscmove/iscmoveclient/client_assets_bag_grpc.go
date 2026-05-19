// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

package iscmoveclient

// gRPC-backed implementation of GetAssetsBagWithBalances.
// Used when a gRPC client is attached to Client.
//
// The AssetsBag on-chain is a Move dynamic-field collection where:
//   - Coin entries: kind=FIELD, name type=0x1::ascii::String (coin type string),
//     value type=0x2::balance::Balance<T>, value BCS = u64 (little-endian balance)
//   - Object entries: kind=OBJECT, name type=0x2::object::ID,
//     child_id = ObjectID of the owned object, value_type = full Move object type

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/iotaledger/bcs-go"

	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
	"github.com/iotaledger/wasp/clients/iscmove"
)

func (c *Client) getAssetsBagWithBalancesGRPC(
	ctx context.Context,
	assetsBagID *iotago.ObjectID,
) (*iscmove.AssetsBagWithBalances, error) {
	fields, err := c.grpcClient.GetDynamicFields(ctx, assetsBagID)
	if err != nil {
		return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: GetDynamicFields: %w", err)
	}

	if os.Getenv("DEBUG") != "" {
		fmt.Fprintf(os.Stderr, "getAssetsBagWithBalancesGRPC: assetsBagID=%s fields=%d\n", assetsBagID, len(fields))
		for i, df := range fields {
			fmt.Fprintf(os.Stderr, "  field[%d]: valueType=%q valueBCSLen=%d childID=%v\n", i, df.ValueType, len(df.ValueBCS), df.ChildID)
		}
	}

	bag := iscmove.AssetsBagWithBalances{
		AssetsBag: iscmove.AssetsBag{
			ID:   *assetsBagID,
			Size: uint64(len(fields)),
		},
		Assets: *iscmove.NewEmptyAssets(),
	}

	for _, df := range fields {
		if df.ValueType == "" {
			continue
		}
		// Parse the value type — handles both short (0x2) and full (0x0000...0002) address forms.
		rt, err := iotago.NewResourceType(df.ValueType)
		if err != nil {
			return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse value type %q: %w", df.ValueType, err)
		}

		if rt.Module == "balance" && rt.ObjectName == "Balance" && rt.SubType1 != nil {
			// Coin field: Balance<T> — extract the inner coin type.
			coinType, err := iotajsonrpc.CoinTypeFromString(rt.SubType1.String())
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse coin type: %w", err)
			}
			balance, err := decodeBalanceBCS(df.ValueBCS)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: decode balance for %s: %w", coinType, err)
			}
			bag.SetCoin(coinType, iotajsonrpc.CoinValue(balance))
		} else if df.ChildID != nil {
			// Dynamic object field — child_id is the owned object's ID.
			typ, err := iotago.ObjectTypeFromString(df.ValueType)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse object type %q: %w", df.ValueType, err)
			}
			bag.AddObject(*df.ChildID, typ)
		}
		// Other entries are unexpected; skip.
	}

	return &bag, nil
}

// decodeBalanceBCS decodes a BCS-encoded Move Balance<T> value.
// Balance<T> is a struct with a single u64 field `value`.
func decodeBalanceBCS(data []byte) (uint64, error) {
	if len(data) == 8 {
		// Fast path: raw u64 little-endian (most common encoding).
		return binary.LittleEndian.Uint64(data), nil
	}
	// Balance<T> has exactly one field: value u64.
	v, err := bcs.Unmarshal[uint64](data)
	if err != nil {
		return 0, fmt.Errorf("decodeBalanceBCS: %w", err)
	}
	return v, nil
}
