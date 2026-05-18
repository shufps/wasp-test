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
	"strings"

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

	bag := iscmove.AssetsBagWithBalances{
		AssetsBag: iscmove.AssetsBag{
			ID:   *assetsBagID,
			Size: uint64(len(fields)),
		},
		Assets: *iscmove.NewEmptyAssets(),
	}

	for _, df := range fields {
		// Distinguish coin fields from object fields by value_type prefix.
		// Coin: value_type = "0x2::balance::Balance<...>"
		// Object: value_type = the full object type, child_id is set.
		if strings.HasPrefix(df.ValueType, "0x2::balance::Balance<") {
			// Extract coin type from "0x2::balance::Balance<{coinType}>"
			coinTypeStr := df.ValueType[len("0x2::balance::Balance<") : len(df.ValueType)-1]

			coinType, err := iotajsonrpc.CoinTypeFromString(coinTypeStr)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: parse coin type %q: %w", coinTypeStr, err)
			}

			// ValueBCS for a Balance<T> is BCS-encoded u64 (the balance value).
			balance, err := decodeBalanceBCS(df.ValueBCS)
			if err != nil {
				return nil, fmt.Errorf("gRPC GetAssetsBagWithBalances: decode balance for %s: %w", coinTypeStr, err)
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
		// Entries with neither a Balance prefix nor a child_id are unexpected; skip.
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
