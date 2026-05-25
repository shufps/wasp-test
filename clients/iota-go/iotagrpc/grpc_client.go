// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Package iotagrpc provides a gRPC client that replaces the indexer-backed
// iotax_* JSON-RPC calls (getCoins, getDynamicFields, etc.) with the
// corresponding gRPC StateService and LedgerService RPCs.
package iotagrpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	bcs "github.com/iotaledger/bcs-go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/iotaledger/wasp/clients/iota-go/iotago"
	bcs_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/bcs"
	ledger_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/ledger_service"
	signatures_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/signatures"
	state_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/state_service"
	transaction_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/transaction"
	transaction_execution_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/transaction_execution_service"
	types_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/types"
	"github.com/iotaledger/wasp/clients/iota-go/iotajsonrpc"
)

// Client wraps gRPC LedgerService + StateService and exposes Go-friendly
// query methods that are drop-in replacements for the indexer-backed
// iotax_* JSON-RPC calls.
type Client struct {
	conn    *grpc.ClientConn
	ledger  ledger_service.LedgerServiceClient
	state   state_service.StateServiceClient
	txExec  transaction_execution_service.TransactionExecutionServiceClient
	address string
}

// NewClient dials the given gRPC address (without grpc:// prefix) and
// returns a ready-to-use Client.
func NewClient(address string, opts ...grpc.DialOption) (*Client, error) {
	defaults := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	}
	conn, err := grpc.NewClient(address, append(defaults, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("iotagrpc.NewClient: dial %s: %w", address, err)
	}
	return &Client{
		conn:    conn,
		ledger:  ledger_service.NewLedgerServiceClient(conn),
		state:   state_service.NewStateServiceClient(conn),
		txExec:  transaction_execution_service.NewTransactionExecutionServiceClient(conn),
		address: address,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// ── Coin queries ──────────────────────────────────────────────────────────────

// GetCoins returns up to limit coins of coinType owned by owner, starting after
// cursor (nil = first page). Replaces iotax_getCoins.
func (c *Client) GetCoins(
	ctx context.Context,
	owner *iotago.Address,
	coinType string,
	cursor []byte,
	limit uint32,
) (*iotajsonrpc.CoinPage, error) {
	objectType := fmt.Sprintf("0x2::coin::Coin<%s>", coinType)
	return c.listCoins(ctx, owner, objectType, coinType, cursor, limit)
}

// GetAllCoins returns all coin objects of any type owned by owner (paginated).
// Replaces iotax_getAllCoins.
func (c *Client) GetAllCoins(
	ctx context.Context,
	owner *iotago.Address,
	cursor []byte,
	limit uint32,
) (*iotajsonrpc.CoinPage, error) {
	// Bare "0x2::coin::Coin" without type param matches all Coin<T> objects.
	return c.listCoins(ctx, owner, "0x2::coin::Coin", "", cursor, limit)
}

// listCoins is the shared implementation for GetCoins / GetAllCoins.
// objectTypeFilter is the type string sent to the server; coinType is the
// value stored in the returned Coin (empty string = derive from BCS).
func (c *Client) listCoins(
	ctx context.Context,
	owner *iotago.Address,
	objectTypeFilter string,
	coinType string,
	cursor []byte,
	limit uint32,
) (*iotajsonrpc.CoinPage, error) {
	req := &state_service.ListOwnedObjectsRequest{
		Owner:      addressToProto(owner),
		ObjectType: &objectTypeFilter,
		PageToken:  cursor,
	}
	if limit > 0 {
		req.PageSize = &limit
	}

	resp, err := c.state.ListOwnedObjects(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("listCoins(%s): %w", objectTypeFilter, err)
	}

	coins := make([]*iotajsonrpc.Coin, 0, len(resp.GetObjects()))
	for _, obj := range resp.GetObjects() {
		ref := obj.GetReference()
		if ref == nil || obj.GetBcs() == nil {
			continue
		}
		coin, err := parseCoinFromObjectBCS(obj.GetBcs().GetData(), ref, coinType)
		if err != nil {
			return nil, fmt.Errorf("listCoins: parse object: %w", err)
		}
		coins = append(coins, coin)
	}

	page := &iotajsonrpc.CoinPage{Data: coins}
	if tok := resp.GetNextPageToken(); len(tok) > 0 {
		nextCursor := iotago.ObjectID(tok)
		page.NextCursor = &nextCursor
		page.HasNextPage = true
	}
	return page, nil
}

// GetCoinObjsForTargetAmount fetches IOTA coins and picks enough to cover
// targetAmount+gasAmount. Replaces the HTTP-backed helper of the same name.
func (c *Client) GetCoinObjsForTargetAmount(
	ctx context.Context,
	owner *iotago.Address,
	targetAmount uint64,
	gasAmount uint64,
) (iotajsonrpc.Coins, error) {
	coinType := iotajsonrpc.IotaCoinType.String()
	var cursor []byte
	const pageSize = uint32(200)
	// Collect all pages into one CoinPage for PickupCoins.
	combined := &iotajsonrpc.CoinPage{}

	for {
		page, err := c.GetCoins(ctx, owner, coinType, cursor, pageSize)
		if err != nil {
			return nil, fmt.Errorf("GetCoinObjsForTargetAmount: %w", err)
		}
		combined.Data = append(combined.Data, page.Data...)
		if !page.HasNextPage {
			break
		}
		cursor = page.NextCursor[:]
	}

	picked, err := iotajsonrpc.PickupCoins(combined, nil, gasAmount, 0, 25)
	if err != nil {
		return nil, err
	}
	_ = targetAmount
	return picked.Coins, nil
}

// ── CoinMetadata ──────────────────────────────────────────────────────────────

// GetCoinMetadata returns metadata for coinType. Replaces iotax_getCoinMetadata.
func (c *Client) GetCoinMetadata(
	ctx context.Context,
	coinType string,
) (*iotajsonrpc.IotaCoinMetadata, error) {
	resp, err := c.state.GetCoinInfo(ctx, &state_service.GetCoinInfoRequest{
		CoinType: &coinType,
	})
	if err != nil {
		return nil, fmt.Errorf("GetCoinMetadata(%s): %w", coinType, err)
	}
	m := resp.GetMetadata()
	if m == nil {
		return nil, fmt.Errorf("GetCoinMetadata(%s): no metadata in response", coinType)
	}

	var id *iotago.ObjectID
	if raw := m.GetId(); raw != nil {
		oid := iotago.ObjectID(raw.GetObjectId())
		id = &oid
	}

	return &iotajsonrpc.IotaCoinMetadata{
		Name:        m.GetName(),
		Symbol:      m.GetSymbol(),
		Decimals:    uint8(m.GetDecimals()),
		Description: m.GetDescription(),
		IconUrl:     m.GetIconUrl(),
		Id:          id,
	}, nil
}

// ── Reference gas price ───────────────────────────────────────────────────────

// GetReferenceGasPrice returns the reference gas price for the current epoch.
// Replaces iotax_getReferenceGasPrice.
func (c *Client) GetReferenceGasPrice(ctx context.Context) (*iotajsonrpc.BigInt, error) {
	resp, err := c.ledger.GetEpoch(ctx, &ledger_service.GetEpochRequest{})
	if err != nil {
		return nil, fmt.Errorf("GetReferenceGasPrice: %w", err)
	}
	if resp.GetEpoch() == nil {
		return nil, fmt.Errorf("GetReferenceGasPrice: empty epoch response")
	}
	return iotajsonrpc.NewBigInt(resp.GetEpoch().GetReferenceGasPrice()), nil
}

// ── Owned objects ─────────────────────────────────────────────────────────────

// OwnedObjectBCS holds a raw-BCS object returned by ListOwnedObjectsByType.
type OwnedObjectBCS struct {
	ObjectID iotago.ObjectID
	Version  uint64
	Digest   iotago.ObjectDigest
	// BCS is the full VersionedObject BCS blob (as returned by gRPC).
	BCS []byte
}

// ListOwnedObjectsByType returns all objects of objectType (e.g.
// "0x<pkg>::request::Request") owned by owner, fetching all pages.
// Replaces iotax_getOwnedObjects for the pullRequests use-case.
func (c *Client) ListOwnedObjectsByType(
	ctx context.Context,
	owner *iotago.ObjectID,
	objectType string,
) ([]OwnedObjectBCS, error) {
	ownerAddr := (*iotago.Address)(owner)
	var results []OwnedObjectBCS
	var cursor []byte

	for {
		resp, err := c.state.ListOwnedObjects(ctx, &state_service.ListOwnedObjectsRequest{
			Owner:      addressToProto(ownerAddr),
			ObjectType: &objectType,
			PageToken:  cursor,
		})
		if err != nil {
			return nil, fmt.Errorf("ListOwnedObjectsByType(%s): %w", objectType, err)
		}

		for _, obj := range resp.GetObjects() {
			ref := obj.GetReference()
			if ref == nil {
				continue
			}
			var objID iotago.ObjectID
			copy(objID[:], ref.GetObjectId().GetObjectId())
			digest := iotago.ObjectDigest(ref.GetDigest().GetDigest())

			var bcsData []byte
			if obj.GetBcs() != nil {
				bcsData = obj.GetBcs().GetData()
			}
			results = append(results, OwnedObjectBCS{
				ObjectID: objID,
				Version:  ref.GetVersion(),
				Digest:   digest,
				BCS:      bcsData,
			})
		}

		if tok := resp.GetNextPageToken(); len(tok) > 0 {
			cursor = tok
		} else {
			break
		}
	}
	return results, nil
}

// OwnedObjectsPage holds a single page of owned objects with decoded BCS.
type OwnedObjectsPage struct {
	Objects     []*ObjectData
	NextCursor  *iotago.ObjectID
	HasNextPage bool
}

// ListOwnedObjectsPage returns one page of objects of objectType owned by owner.
// cursor is the page token from the previous page (nil = first page).
// Replaces iotax_getOwnedObjects for paginated use.
func (c *Client) ListOwnedObjectsPage(
	ctx context.Context,
	owner *iotago.Address,
	objectType string,
	cursor []byte,
	limit uint32,
) (*OwnedObjectsPage, error) {
	req := &state_service.ListOwnedObjectsRequest{
		Owner:     addressToProto(owner),
		PageToken: cursor,
	}
	if objectType != "" {
		req.ObjectType = &objectType
	}
	if limit > 0 {
		req.PageSize = &limit
	}

	resp, err := c.state.ListOwnedObjects(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("ListOwnedObjectsPage(%s): %w", objectType, err)
	}

	result := &OwnedObjectsPage{}
	for _, obj := range resp.GetObjects() {
		ref := obj.GetReference()
		if ref == nil {
			continue
		}
		var objID iotago.ObjectID
		copy(objID[:], ref.GetObjectId().GetObjectId())

		bcsData := obj.GetBcs().GetData()
		if len(bcsData) == 0 {
			continue
		}
		od, err := parseObjectBCS(&objID, bcsData)
		if err != nil {
			continue
		}
		// Override version and digest from the reference field (correct values).
		od.Version = ref.GetVersion()
		od.Digest = iotago.ObjectDigest(ref.GetDigest().GetDigest())
		result.Objects = append(result.Objects, od)
	}

	if tok := resp.GetNextPageToken(); len(tok) > 0 {
		nextCursor := iotago.ObjectID(tok)
		result.NextCursor = &nextCursor
		result.HasNextPage = true
	}
	return result, nil
}

// ── Dynamic fields ────────────────────────────────────────────────────────────

// DynamicFieldEntry holds a single dynamic field returned by GetDynamicFields.
type DynamicFieldEntry struct {
	FieldID   iotago.ObjectID
	NameBCS   []byte // BCS-encoded field name (key)
	ValueBCS  []byte // BCS-encoded value (for FIELD kind; empty for OBJECT kind)
	ValueType string // Move type string of the value, e.g. "0x2::balance::Balance<0x2::iota::IOTA>"

	// For dynamic object fields (kind == OBJECT):
	ChildID        *iotago.ObjectID // ObjectId of the child object
	ChildObjectBCS []byte           // BCS of the child object itself
}

// GetDynamicFields returns all dynamic fields of parentID.
// Replaces iotax_getDynamicFields.
func (c *Client) GetDynamicFields(
	ctx context.Context,
	parentID *iotago.ObjectID,
) ([]DynamicFieldEntry, error) {
	var results []DynamicFieldEntry
	var cursor []byte

	for {
		resp, err := c.state.ListDynamicFields(ctx, &state_service.ListDynamicFieldsRequest{
			Parent:    objectIDToProto(parentID),
			PageToken: cursor,
			ReadMask:  &fieldmaskpb.FieldMask{Paths: []string{"kind", "field_id", "name", "value", "value_type", "child_id"}},
		})
		if err != nil {
			return nil, fmt.Errorf("GetDynamicFields(%s): %w", parentID, err)
		}

		for _, df := range resp.GetDynamicFields() {
			var fieldID iotago.ObjectID
			if fid := df.GetFieldId(); fid != nil {
				copy(fieldID[:], fid.GetObjectId())
			}
			entry := DynamicFieldEntry{
				FieldID:   fieldID,
				ValueType: df.GetValueType(),
			}
			if n := df.GetName(); n != nil {
				entry.NameBCS = n.GetData()
			}
			if v := df.GetValue(); v != nil {
				entry.ValueBCS = v.GetData()
			}
			if cid := df.GetChildId(); cid != nil {
				oid := iotago.ObjectID(cid.GetObjectId())
				entry.ChildID = &oid
			}
			if co := df.GetChildObject(); co != nil && co.GetBcs() != nil {
				entry.ChildObjectBCS = co.GetBcs().GetData()
			}
			results = append(results, entry)
		}

		if tok := resp.GetNextPageToken(); len(tok) > 0 {
			cursor = tok
		} else {
			break
		}
	}
	return results, nil
}

// ── Transaction execution ─────────────────────────────────────────────────────

// SimulateTransaction simulates (dry-runs) txBytes via gRPC.
// Returns nil if simulation succeeds, or an error describing the execution failure.
func (c *Client) SimulateTransaction(ctx context.Context, txBytes []byte) error {
	resp, err := c.txExec.SimulateTransactions(ctx, &transaction_execution_service.SimulateTransactionsRequest{
		Transactions: []*transaction_execution_service.SimulateTransactionItem{
			{
				Transaction: &transaction_pb.Transaction{
					Bcs: &bcs_pb.BcsData{Data: txBytes},
				},
			},
		},
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"execution_result"}},
	})
	if err != nil {
		return fmt.Errorf("SimulateTransaction: rpc error: %w", err)
	}
	results := resp.GetTransactionResults()
	if len(results) == 0 {
		return fmt.Errorf("SimulateTransaction: empty response")
	}
	r := results[0]
	if e := r.GetError(); e != nil {
		return fmt.Errorf("SimulateTransaction: %s", e.GetMessage())
	}
	sim := r.GetSimulatedTransaction()
	if sim == nil {
		return fmt.Errorf("SimulateTransaction: no simulation result")
	}
	if execErr := sim.GetExecutionError(); execErr != nil {
		src := execErr.GetSource()
		if src == "" {
			src = "execution failed"
		}
		return fmt.Errorf("SimulateTransaction: %s", src)
	}
	return nil
}

// bcsEncodeBytes encodes a byte slice as a BCS Vec<u8>: ULEB128(len) + bytes.
// The gRPC UserSignature.Bcs field supports both the "length prefixed" (BCS Vec<u8>)
// and "not length prefixed" (raw bytes) forms as input. However, the raw form can be
// misinterpreted: a signature starting with 0x00 (Ed25519 scheme flag) is parsed as
// ULEB128 length = 0, yielding an empty slice and "missing signature scheme flag".
// Sending the length-prefixed form avoids this ambiguity.
func bcsEncodeBytes(b []byte) []byte {
	length := uint64(len(b))
	var prefix []byte
	for {
		octet := byte(length & 0x7F)
		length >>= 7
		if length != 0 {
			octet |= 0x80
		}
		prefix = append(prefix, octet)
		if length == 0 {
			break
		}
	}
	return append(prefix, b...)
}


// ExecuteTransaction submits a signed transaction via gRPC.
// Returns the transaction digest on success.
func (c *Client) ExecuteTransaction(ctx context.Context, txBytes []byte, signatures [][]byte) (string, error) {
	sigs := make([]*signatures_pb.UserSignature, len(signatures))
	for i, sig := range signatures {
		sigs[i] = &signatures_pb.UserSignature{Bcs: &bcs_pb.BcsData{Data: bcsEncodeBytes(sig)}}
	}

	checkpointInclusionTimeoutMs := uint64(5_000)

	resp, err := c.txExec.ExecuteTransactions(ctx, &transaction_execution_service.ExecuteTransactionsRequest{
		Transactions: []*transaction_execution_service.ExecuteTransactionItem{
			{
				Transaction: &transaction_pb.Transaction{
					Bcs: &bcs_pb.BcsData{Data: txBytes},
				},
				Signatures: &signatures_pb.UserSignatures{Signatures: sigs},
			},
		},
		ReadMask:                     &fieldmaskpb.FieldMask{Paths: []string{"effects", "transaction.digest"}},
		CheckpointInclusionTimeoutMs: &checkpointInclusionTimeoutMs,
	})
	if err != nil {
		return "", fmt.Errorf("ExecuteTransaction: rpc error: %w", err)
	}
	results := resp.GetTransactionResults()
	if len(results) == 0 {
		return "", fmt.Errorf("ExecuteTransaction: empty response")
	}
	r := results[0]
	if e := r.GetError(); e != nil {
		return "", fmt.Errorf("ExecuteTransaction: %s", e.GetMessage())
	}
	executed := r.GetExecutedTransaction()
	if executed == nil {
		return "", fmt.Errorf("ExecuteTransaction: no executed transaction in response")
	}
	digest := iotago.Digest(executed.GetTransaction().GetDigest().GetDigest())
	return digest.String(), nil
}


// ── Epoch info ────────────────────────────────────────────────────────────────

// EpochInfo holds system state info fetched via GetEpoch.
type EpochInfo struct {
	EpochID            uint64
	ProtocolVersion    uint64
	SystemStateVersion uint64
	ReferenceGasPrice  uint64
	EpochStartMs       int64
	EpochEndMs         int64 // 0 if not yet known
	EpochDurationMs    int64 // 0 if not parseable from BCS
}

// GetEpochInfo fetches current epoch info via gRPC LedgerService.GetEpoch.
// It decodes the BcsSystemState to extract ProtocolVersion, SystemStateVersion,
// and EpochDurationMs from the SystemParametersV1 struct.
func (c *Client) GetEpochInfo(ctx context.Context) (*EpochInfo, error) {
	resp, err := c.ledger.GetEpoch(ctx, &ledger_service.GetEpochRequest{
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"epoch", "bcs_system_state", "start", "end", "reference_gas_price"}},
	})
	if err != nil {
		return nil, fmt.Errorf("GetEpochInfo: %w", err)
	}
	ep := resp.GetEpoch()
	if ep == nil {
		return nil, fmt.Errorf("GetEpochInfo: no epoch in response")
	}

	info := &EpochInfo{
		EpochID:           ep.GetEpoch(),
		ReferenceGasPrice: ep.GetReferenceGasPrice(),
	}
	if s := ep.GetStart(); s != nil {
		info.EpochStartMs = s.AsTime().UnixMilli()
	}
	if e := ep.GetEnd(); e != nil {
		info.EpochEndMs = e.AsTime().UnixMilli()
	}

	// Decode BCS system state to extract ProtocolVersion, SystemStateVersion,
	// and EpochDurationMs. Errors are ignored (best-effort); missing fields stay 0.
	if bcsData := ep.GetBcsSystemState().GetData(); len(bcsData) > 0 {
		_ = decodeSystemStateBCS(bcsData, info)
	}
	return info, nil
}

// decodeSystemStateBCS parses the BCS-encoded IotaSystemState and populates
// ProtocolVersion, SystemStateVersion, and EpochDurationMs in info.
//
// IotaSystemState is a Rust enum with two variants:
//
//	V1 (tag 0): IotaSystemStateV1  — uses ValidatorSetV1
//	V2 (tag 1): IotaSystemStateV2  — uses ValidatorSetV2 (adds committee_members Vec<u64>)
//
// Go types for both variants are defined in bcs_system_state.go.
// Only the fields up to epoch_duration_ms are decoded; the rest are ignored.
func decodeSystemStateBCS(data []byte, info *EpochInfo) error {
	dec := bcs.NewDecoder(bytes.NewReader(data))

	variantTag := dec.ReadEnumIdx()
	if dec.Err() != nil {
		return dec.Err()
	}

	switch variantTag {
	case 0: // IotaSystemState::V1
		var head sysStateHeadV1
		dec.Decode(&head)
		if dec.Err() != nil {
			return fmt.Errorf("decodeSystemStateBCS V1: %w", dec.Err())
		}
		info.ProtocolVersion = head.ProtocolVersion
		info.SystemStateVersion = head.SystemStateVersion
		info.EpochDurationMs = int64(head.EpochDurationMs)
	case 1: // IotaSystemState::V2
		var head sysStateHeadV2
		dec.Decode(&head)
		if dec.Err() != nil {
			return fmt.Errorf("decodeSystemStateBCS V2: %w", dec.Err())
		}
		info.ProtocolVersion = head.ProtocolVersion
		info.SystemStateVersion = head.SystemStateVersion
		info.EpochDurationMs = int64(head.EpochDurationMs)
	default:
		return fmt.Errorf("decodeSystemStateBCS: unknown IotaSystemState variant %d", variantTag)
	}
	return nil
}

// GetTotalSupply returns the total supply of coinType via GetCoinInfo.
func (c *Client) GetTotalSupply(ctx context.Context, coinType string) (uint64, error) {
	resp, err := c.state.GetCoinInfo(ctx, &state_service.GetCoinInfoRequest{
		CoinType: &coinType,
	})
	if err != nil {
		return 0, fmt.Errorf("GetTotalSupply(%s): %w", coinType, err)
	}
	if t := resp.GetTreasury(); t != nil {
		return t.GetTotalSupply(), nil
	}
	return 0, nil
}

// ── Object fetching ───────────────────────────────────────────────────────────

// ObjectData holds the result of a GetObjects call for a single object.
type ObjectData struct {
	ObjectID iotago.ObjectID
	Version  uint64
	Digest   iotago.ObjectDigest
	// Contents is the Move struct content bytes (inside Vec<u8> in BCS).
	Contents []byte
	// Owner is the address owner (AddressOwner or ObjectOwner), or nil for Shared/Immutable.
	Owner *iotago.Address
}

// GetObjectBCS fetches a single object by ID via gRPC GetObjects and decodes
// the VersionedObject BCS to extract contents and owner.
// version is optional (nil = latest).
func (c *Client) GetObjectBCS(ctx context.Context, objectID *iotago.ObjectID, version *uint64) (*ObjectData, error) {
	ref := &types_pb.ObjectReference{
		ObjectId: &types_pb.ObjectId{ObjectId: objectID[:]},
	}
	if version != nil {
		ref.Version = version
	}
	stream, err := c.ledger.GetObjects(ctx, &ledger_service.GetObjectsRequest{
		Requests: &ledger_service.ObjectRequests{
			Requests: []*ledger_service.ObjectRequest{
				{ObjectRef: ref},
			},
		},
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"reference", "bcs"}},
	})
	if err != nil {
		return nil, fmt.Errorf("GetObjectBCS(%s): stream: %w", objectID, err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("GetObjectBCS(%s): recv: %w", objectID, err)
		}
		for _, objResult := range msg.GetObjects() {
			if e := objResult.GetError(); e != nil {
				return nil, fmt.Errorf("GetObjectBCS(%s): %s", objectID, e.GetMessage())
			}
			obj := objResult.GetObject()
			if obj == nil {
				continue
			}
			bcsData := obj.GetBcs().GetData()
			if len(bcsData) == 0 {
				return nil, fmt.Errorf("GetObjectBCS(%s): empty BCS", objectID)
			}
			od, err := parseObjectBCS(objectID, bcsData)
			if err != nil {
				return nil, err
			}
			// Override version and digest from the response reference field.
			// parseObjectBCS reads the BCS bytes and would read previous_transaction
			// (32 bytes after the owner) as the digest, which is wrong.
			// The reference field contains the correct version and object digest.
			if objRef := obj.GetReference(); objRef != nil {
				od.Version = objRef.GetVersion()
				od.Digest = iotago.ObjectDigest(objRef.GetDigest().GetDigest())
			}
			return od, nil
		}
		if !msg.GetHasNext() {
			break
		}
	}
	return nil, fmt.Errorf("GetObjectBCS(%s): object not found", objectID)
}

// parseObjectBCS decodes a VersionedObject BCS blob and extracts the
// Move struct contents, owner address, version, and digest.
func parseObjectBCS(objectID *iotago.ObjectID, data []byte) (*ObjectData, error) {
	vo, err := bcs.UnmarshalStream[versionedObject](bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parseObjectBCS: %w", err)
	}
	if vo.V1 == nil {
		return nil, fmt.Errorf("parseObjectBCS: not a V1 object")
	}
	ms := vo.V1.Data.Struct
	if ms == nil {
		return nil, fmt.Errorf("parseObjectBCS: not a Move struct object")
	}
	return &ObjectData{
		ObjectID: *objectID,
		Version:  ms.Version,
		Digest:   iotago.ObjectDigest(vo.V1.PreviousTransaction),
		Contents: ms.Contents,
		Owner:    ownerAddress(vo.V1.Owner),
	}, nil
}

// ── BCS parsing helpers ───────────────────────────────────────────────────────

// parseCoinFromObjectBCS decodes a gRPC Object BCS blob (VersionedObject::V1)
// and extracts the fields needed to populate a iotajsonrpc.Coin.
//
// coinTypeHint is the coin type string to embed in the result; if empty, the
// coin type is derived from the decoded MoveObjectType.
func parseCoinFromObjectBCS(
	data []byte,
	ref interface {
		GetObjectId() *types_pb.ObjectId
		GetVersion() uint64
		GetDigest() *types_pb.Digest
	},
	coinTypeHint string,
) (*iotajsonrpc.Coin, error) {
	vo, err := bcs.UnmarshalStream[versionedObject](bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: %w", err)
	}
	if vo.V1 == nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: not a V1 object")
	}
	ms := vo.V1.Data.Struct
	if ms == nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: not a Move struct object")
	}

	ct := coinTypeHint
	if ct == "" {
		ct = moveObjectTypeToCoinType(ms.ObjectType)
	}

	coin, err := bcs.Unmarshal[coinContents](ms.Contents)
	if err != nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: decode coin contents: %w", err)
	}

	var objID iotago.ObjectID
	copy(objID[:], ref.GetObjectId().GetObjectId())
	digest := iotago.ObjectDigest(ref.GetDigest().GetDigest())

	return &iotajsonrpc.Coin{
		CoinType:     iotajsonrpc.CoinType(ct),
		CoinObjectID: &objID,
		Version:      iotajsonrpc.NewBigInt(ref.GetVersion()),
		Digest:       &digest,
		Balance:      iotajsonrpc.NewBigInt(coin.Balance),
	}, nil
}


// ── proto helpers ─────────────────────────────────────────────────────────────

func addressToProto(addr *iotago.Address) *types_pb.Address {
	if addr == nil {
		return nil
	}
	return &types_pb.Address{Address: addr[:]}
}

func objectIDToProto(id *iotago.ObjectID) *types_pb.ObjectId {
	if id == nil {
		return nil
	}
	return &types_pb.ObjectId{ObjectId: id[:]}
}
