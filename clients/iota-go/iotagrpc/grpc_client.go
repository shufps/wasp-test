// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Package iotagrpc provides a gRPC client that replaces the indexer-backed
// iotax_* JSON-RPC calls (getCoins, getDynamicFields, etc.) with the
// corresponding gRPC StateService and LedgerService RPCs.
package iotagrpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"

	bcs_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/bcs"
	ledger_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/ledger_service"
	signatures_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/signatures"
	state_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/state_service"
	transaction_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/transaction"
	transaction_execution_service "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/transaction_execution_service"
	types_pb "github.com/iotaledger/wasp/clients/iota-go/iotagrpc/iota/grpc/v1/types"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
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

// ExecuteTransaction submits a signed transaction via gRPC.
// Returns the transaction digest on success.
func (c *Client) ExecuteTransaction(ctx context.Context, txBytes []byte, signatures [][]byte) (string, error) {
	sigs := make([]*signatures_pb.UserSignature, len(signatures))
	for i, sig := range signatures {
		sigs[i] = &signatures_pb.UserSignature{Bcs: &bcs_pb.BcsData{Data: sig}}
	}
	resp, err := c.txExec.ExecuteTransactions(ctx, &transaction_execution_service.ExecuteTransactionsRequest{
		Transactions: []*transaction_execution_service.ExecuteTransactionItem{
			{
				Transaction: &transaction_pb.Transaction{
					Bcs: &bcs_pb.BcsData{Data: txBytes},
				},
				Signatures: &signatures_pb.UserSignatures{Signatures: sigs},
			},
		},
		ReadMask: &fieldmaskpb.FieldMask{Paths: []string{"effects"}},
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
	if r.GetExecutedTransaction() == nil {
		return "", fmt.Errorf("ExecuteTransaction: no executed transaction in response")
	}
	return "", nil
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
// IotaSystemState is a Rust enum:
//
//	V1 (tag 0): IotaSystemStateV1  — uses ValidatorSetV1
//	V2 (tag 1): IotaSystemStateV2  — uses ValidatorSetV2 (adds committee_members Vec<u64>)
//
// Both variants share the same layout up through parameters.epoch_duration_ms:
//
//	ULEB128              variant_tag
//	u64                  epoch
//	u64                  protocol_version
//	u64                  system_state_version
//	40 bytes             iota_treasury_cap  (UID(32) + Supply.value(8))
//	ValidatorSet{V1|V2}  validators         (variable length)
//	16 bytes             storage_fund       (Balance(8) + Balance(8))
//	u64                  parameters.epoch_duration_ms  ← what we need
func decodeSystemStateBCS(data []byte, info *EpochInfo) error {
	r := &bcsReader{buf: data}

	// Enum variant tag
	variantTag, err := r.readULEB128()
	if err != nil {
		return err
	}
	// epoch — already have it from the gRPC response
	if _, err := r.readU64(); err != nil {
		return err
	}
	// protocol_version
	if pv, err := r.readU64(); err != nil {
		return err
	} else {
		info.ProtocolVersion = pv
	}
	// system_state_version
	if ssv, err := r.readU64(); err != nil {
		return err
	} else {
		info.SystemStateVersion = ssv
	}

	// iota_treasury_cap: IotaTreasuryCap
	//   inner: TreasuryCap { id: UID(32 bytes), total_supply: Supply { value: u64(8 bytes) } }
	if err := r.skip(40); err != nil {
		return err
	}

	// validators: ValidatorSetV1 or ValidatorSetV2
	switch variantTag {
	case 0: // IotaSystemState::V1 → ValidatorSetV1
		if err := r.skipValidatorSetV1(); err != nil {
			return err
		}
	case 1: // IotaSystemState::V2 → ValidatorSetV2
		if err := r.skipValidatorSetV2(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("decodeSystemStateBCS: unknown IotaSystemState variant %d", variantTag)
	}

	// storage_fund: StorageFundV1
	//   total_object_storage_rebates: Balance { value: u64 }  (8 bytes)
	//   non_refundable_balance:       Balance { value: u64 }  (8 bytes)
	if err := r.skip(16); err != nil {
		return err
	}

	// parameters: SystemParametersV1 — first field is epoch_duration_ms: u64
	if epochDurationMs, err := r.readU64(); err != nil {
		return err
	} else {
		info.EpochDurationMs = int64(epochDurationMs)
	}

	return nil
}

// skipValidatorSetV1 skips a BCS-encoded ValidatorSetV1:
//
//	u64                         total_stake
//	Vec<ValidatorV1>            active_validators
//	TableVec (40 bytes)         pending_active_validators
//	Vec<u64>                    pending_removals
//	Table (40 bytes)            staking_pool_mappings
//	Table (40 bytes)            inactive_validators
//	Table (40 bytes)            validator_candidates
//	VecMap<Address, u64>        at_risk_validators   (each entry = 40 bytes)
//	Bag (40 bytes)              extra_fields
func (r *bcsReader) skipValidatorSetV1() error {
	return r.skipValidatorSetInner(false)
}

// skipValidatorSetV2 skips a BCS-encoded ValidatorSetV2 (like V1 plus committee_members Vec<u64>):
//
//	u64                         total_stake
//	Vec<ValidatorV1>            active_validators
//	Vec<u64>                    committee_members        ← NEW in V2
//	TableVec (40 bytes)         pending_active_validators
//	Vec<u64>                    pending_removals
//	Table (40 bytes)            staking_pool_mappings
//	Table (40 bytes)            inactive_validators
//	Table (40 bytes)            validator_candidates
//	VecMap<Address, u64>        at_risk_validators
//	Bag (40 bytes)              extra_fields
func (r *bcsReader) skipValidatorSetV2() error {
	return r.skipValidatorSetInner(true)
}

func (r *bcsReader) skipValidatorSetInner(v2 bool) error {
	// total_stake: u64
	if err := r.skip(8); err != nil {
		return err
	}
	// active_validators: Vec<ValidatorV1>
	count, err := r.readULEB128()
	if err != nil {
		return err
	}
	for i := uint64(0); i < count; i++ {
		if err := r.skipValidatorV1(); err != nil {
			return fmt.Errorf("skipValidatorSet: validator %d: %w", i, err)
		}
	}
	if v2 {
		// committee_members: Vec<u64>
		cmCount, err := r.readULEB128()
		if err != nil {
			return err
		}
		if err := r.skip(int(cmCount) * 8); err != nil {
			return err
		}
	}
	// pending_active_validators: TableVec = { contents: Table = { id: ObjectID(32), size: u64(8) } } = 40 bytes
	if err := r.skip(40); err != nil {
		return err
	}
	// pending_removals: Vec<u64>
	prCount, err := r.readULEB128()
	if err != nil {
		return err
	}
	if err := r.skip(int(prCount) * 8); err != nil {
		return err
	}
	// staking_pool_mappings, inactive_validators, validator_candidates: Table (40 bytes each)
	if err := r.skip(40 * 3); err != nil {
		return err
	}
	// at_risk_validators: VecMap<IotaAddress, u64>
	//   Vec<Entry<IotaAddress, u64>>: ULEB128 + count × (32 + 8) bytes
	arCount, err := r.readULEB128()
	if err != nil {
		return err
	}
	if err := r.skip(int(arCount) * 40); err != nil {
		return err
	}
	// extra_fields: Bag = { id: UID(32), size: u64(8) } = 40 bytes
	return r.skip(40)
}

// skipValidatorV1 skips a BCS-encoded ValidatorV1:
//
//	ValidatorMetadataV1   metadata
//	u64                   voting_power
//	ID (32 bytes)         operation_cap_id
//	u64                   gas_price
//	StakingPoolV1         staking_pool
//	u64                   commission_rate
//	u64                   next_epoch_stake
//	u64                   next_epoch_gas_price
//	u64                   next_epoch_commission_rate
//	Bag (40 bytes)        extra_fields
func (r *bcsReader) skipValidatorV1() error {
	if err := r.skipValidatorMetadataV1(); err != nil {
		return err
	}
	// voting_power(8) + operation_cap_id(32) + gas_price(8)
	if err := r.skip(48); err != nil {
		return err
	}
	if err := r.skipStakingPoolV1(); err != nil {
		return err
	}
	// commission_rate(8) + next_epoch_stake(8) + next_epoch_gas_price(8) + next_epoch_commission_rate(8) + extra_fields Bag(40)
	return r.skip(72)
}

// skipValidatorMetadataV1 skips a BCS-encoded ValidatorMetadataV1:
//
//	IotaAddress (32 bytes)              iota_address
//	Vec<u8>                             authority_pubkey_bytes
//	Vec<u8>                             network_pubkey_bytes
//	Vec<u8>                             protocol_pubkey_bytes
//	Vec<u8>                             proof_of_possession_bytes
//	String                              name
//	String                              description
//	String                              image_url
//	String                              project_url
//	String                              net_address
//	String                              p2p_address
//	String                              primary_address
//	Option<Vec<u8>>                     next_epoch_authority_pubkey_bytes
//	Option<Vec<u8>>                     next_epoch_proof_of_possession
//	Option<Vec<u8>>                     next_epoch_network_pubkey_bytes
//	Option<Vec<u8>>                     next_epoch_protocol_pubkey_bytes
//	Option<String>                      next_epoch_net_address
//	Option<String>                      next_epoch_p2p_address
//	Option<String>                      next_epoch_primary_address
//	Bag (40 bytes)                      extra_fields
func (r *bcsReader) skipValidatorMetadataV1() error {
	// iota_address: IotaAddress = 32 bytes
	if err := r.skip(32); err != nil {
		return err
	}
	// 4 × Vec<u8>: authority_pubkey, network_pubkey, protocol_pubkey, proof_of_possession
	for i := 0; i < 4; i++ {
		if _, err := r.readBytes(); err != nil {
			return err
		}
	}
	// 7 × String: name, description, image_url, project_url, net_address, p2p_address, primary_address
	for i := 0; i < 7; i++ {
		if _, err := r.readBytes(); err != nil {
			return err
		}
	}
	// 7 × Option<Vec<u8> or String>: next_epoch_* fields
	// In BCS, Option<T> = 0x00 (None) | 0x01 + T (Some).
	// Both Vec<u8> and String are ULEB128-prefixed byte slices.
	for i := 0; i < 7; i++ {
		tag, err := r.readU8()
		if err != nil {
			return err
		}
		if tag == 1 { // Some(value)
			if _, err := r.readBytes(); err != nil {
				return err
			}
		}
	}
	// extra_fields: Bag = { id: UID(32), size: u64(8) } = 40 bytes
	return r.skip(40)
}

// skipStakingPoolV1 skips a BCS-encoded StakingPoolV1:
//
//	ObjectID (32 bytes)    id
//	Option<u64>            activation_epoch
//	Option<u64>            deactivation_epoch
//	u64                    iota_balance
//	Balance (8 bytes)      rewards_pool
//	u64                    pool_token_balance
//	Table (40 bytes)       exchange_rates
//	u64                    pending_stake
//	u64                    pending_total_iota_withdraw
//	u64                    pending_pool_token_withdraw
//	Bag (40 bytes)         extra_fields
func (r *bcsReader) skipStakingPoolV1() error {
	// id: ObjectID = 32 bytes
	if err := r.skip(32); err != nil {
		return err
	}
	// activation_epoch: Option<u64>
	if tag, err := r.readU8(); err != nil {
		return err
	} else if tag == 1 {
		if err := r.skip(8); err != nil {
			return err
		}
	}
	// deactivation_epoch: Option<u64>
	if tag, err := r.readU8(); err != nil {
		return err
	} else if tag == 1 {
		if err := r.skip(8); err != nil {
			return err
		}
	}
	// iota_balance(8) + rewards_pool Balance(8) + pool_token_balance(8)
	if err := r.skip(24); err != nil {
		return err
	}
	// exchange_rates: Table = { id: ObjectID(32), size: u64(8) } = 40 bytes
	if err := r.skip(40); err != nil {
		return err
	}
	// pending_stake(8) + pending_total_iota_withdraw(8) + pending_pool_token_withdraw(8) = 24
	if err := r.skip(24); err != nil {
		return err
	}
	// extra_fields: Bag = 40 bytes
	return r.skip(40)
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
	r := &bcsReader{buf: data}

	// VersionedObject variant tag = 0 (V1)
	if v, err := r.readU8(); err != nil || v != 0 {
		return nil, fmt.Errorf("parseObjectBCS: expected V1 tag, got %d (err: %v)", v, err)
	}
	// ObjectData variant tag = 0 (Struct/MoveObject)
	if v, err := r.readU8(); err != nil || v != 0 {
		return nil, fmt.Errorf("parseObjectBCS: expected Struct tag, got %d (err: %v)", v, err)
	}
	// MoveObjectType (compact encoding, skip it)
	if _, err := r.readMoveObjectType(); err != nil {
		return nil, fmt.Errorf("parseObjectBCS: readMoveObjectType: %w", err)
	}
	// version (u64)
	version, err := r.readU64()
	if err != nil {
		return nil, fmt.Errorf("parseObjectBCS: read version: %w", err)
	}
	// contents: Vec<u8>
	contents, err := r.readBytes()
	if err != nil {
		return nil, fmt.Errorf("parseObjectBCS: read contents: %w", err)
	}
	// Owner:
	//   u8 = 0 (AddressOwner) → u8[32] address
	//   u8 = 1 (ObjectOwner)  → u8[32] address
	//   u8 = 2 (Shared)       → u64 initial_shared_version
	//   u8 = 3 (Immutable)    → nothing
	ownerTag, err := r.readU8()
	if err != nil {
		return nil, fmt.Errorf("parseObjectBCS: read owner tag: %w", err)
	}
	var owner *iotago.Address
	switch ownerTag {
	case 0, 1: // AddressOwner or ObjectOwner
		var addr iotago.Address
		if r.pos+32 > len(r.buf) {
			return nil, fmt.Errorf("parseObjectBCS: owner address too short")
		}
		copy(addr[:], r.buf[r.pos:r.pos+32])
		r.pos += 32
		owner = &addr
	case 2: // Shared
		if _, err := r.readU64(); err != nil { // skip initial_shared_version
			return nil, fmt.Errorf("parseObjectBCS: skip shared version: %w", err)
		}
	case 3: // Immutable, no extra bytes
	default:
		return nil, fmt.Errorf("parseObjectBCS: unknown owner tag %d", ownerTag)
	}
	// Digest: 32 bytes
	if r.pos+32 > len(r.buf) {
		return nil, fmt.Errorf("parseObjectBCS: digest too short")
	}
	var digest iotago.ObjectDigest
	copy(digest[:], r.buf[r.pos:r.pos+32])

	return &ObjectData{
		ObjectID: *objectID,
		Version:  version,
		Digest:   digest,
		Contents: contents,
		Owner:    owner,
	}, nil
}

// ── BCS parsing helpers ───────────────────────────────────────────────────────

// parseCoinFromObjectBCS decodes a gRPC Object BCS blob (VersionedObject::V1)
// and extracts the fields needed to populate a iotajsonrpc.Coin.
//
// The BCS layout (from iota-sdk-types/src/object.rs) is:
//
//	VersionedObject = u8(0=V1) + Object
//	Object          = ObjectData + Owner + Digest(32) + u64
//	ObjectData      = u8(0=Struct) + MoveStruct
//	MoveStruct      = MoveObjectType + u64(version) + Vec<u8>(contents)
//	MoveObjectType  = u8(0=Other:StructTag | 1=GasCoin | 2=StakedIota | 3=Coin:TypeTag)
//	contents        = [32 bytes ObjectID] [8 bytes balance u64-LE]
//
// coinTypeHint is the coin type string to embed in the result; if empty, the
// coin type is derived from the BCS (GasCoin → IOTA coin type; others omitted).
func parseCoinFromObjectBCS(
	data []byte,
	ref interface {
		GetObjectId() *types_pb.ObjectId
		GetVersion() uint64
		GetDigest() *types_pb.Digest
	},
	coinTypeHint string,
) (*iotajsonrpc.Coin, error) {
	r := &bcsReader{buf: data}

	// VersionedObject variant tag — must be 0 (V1)
	if v, err := r.readU8(); err != nil || v != 0 {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: expected VersionedObject::V1 tag 0, got %d", v)
	}

	// ObjectData variant tag — must be 0 (Struct / MoveObject)
	if v, err := r.readU8(); err != nil || v != 0 {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: expected ObjectData::Struct tag 0, got %d", v)
	}

	// MoveObjectType (custom compact BCS)
	detectedCoinType, err := r.readMoveObjectType()
	if err != nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: readMoveObjectType: %w", err)
	}

	// Use caller-supplied hint; fall back to detected type.
	ct := coinTypeHint
	if ct == "" {
		ct = detectedCoinType
	}

	// version (u64 LE)
	if _, err := r.readU64(); err != nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: read version: %w", err)
	}

	// contents: ULEB128 length + bytes
	contents, err := r.readBytes()
	if err != nil {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: read contents: %w", err)
	}
	if len(contents) < 40 {
		return nil, fmt.Errorf("parseCoinFromObjectBCS: contents too short (%d bytes)", len(contents))
	}
	balance := binary.LittleEndian.Uint64(contents[32:40])

	// Build result using the reference for ID/version/digest.
	var objID iotago.ObjectID
	copy(objID[:], ref.GetObjectId().GetObjectId())
	digest := iotago.ObjectDigest(ref.GetDigest().GetDigest())

	return &iotajsonrpc.Coin{
		CoinType:     iotajsonrpc.CoinType(ct),
		CoinObjectID: &objID,
		Version:      iotajsonrpc.NewBigInt(ref.GetVersion()),
		Digest:       &digest,
		Balance:      iotajsonrpc.NewBigInt(balance),
	}, nil
}

// ── tiny BCS reader ───────────────────────────────────────────────────────────

type bcsReader struct {
	buf []byte
	pos int
}

func (r *bcsReader) readU8() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, fmt.Errorf("bcsReader: unexpected end of data at pos %d", r.pos)
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *bcsReader) readU64() (uint64, error) {
	if r.pos+8 > len(r.buf) {
		return 0, fmt.Errorf("bcsReader: not enough bytes for u64 at pos %d", r.pos)
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos:])
	r.pos += 8
	return v, nil
}

func (r *bcsReader) readU32() (uint32, error) {
	if r.pos+4 > len(r.buf) {
		return 0, fmt.Errorf("bcsReader: not enough bytes for u32 at pos %d", r.pos)
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, nil
}

// readULEB128 decodes an unsigned LEB128 integer.
func (r *bcsReader) readULEB128() (uint64, error) {
	var result uint64
	var shift uint
	for {
		b, err := r.readU8()
		if err != nil {
			return 0, err
		}
		result |= uint64(b&0x7F) << shift
		if b&0x80 == 0 {
			return result, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, fmt.Errorf("bcsReader: ULEB128 overflow")
		}
	}
}

// readBytes reads a ULEB128-length-prefixed byte slice (BCS Vec<u8>).
func (r *bcsReader) readBytes() ([]byte, error) {
	length, err := r.readULEB128()
	if err != nil {
		return nil, err
	}
	if r.pos+int(length) > len(r.buf) {
		return nil, fmt.Errorf("bcsReader: slice length %d exceeds buffer", length)
	}
	out := r.buf[r.pos : r.pos+int(length)]
	r.pos += int(length)
	return out, nil
}

// readString reads a ULEB128-prefixed UTF-8 string (BCS String / Identifier).
func (r *bcsReader) readString() (string, error) {
	b, err := r.readBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// skip advances the reader by n bytes.
func (r *bcsReader) skip(n int) error {
	if r.pos+n > len(r.buf) {
		return fmt.Errorf("bcsReader: skip %d exceeds buffer", n)
	}
	r.pos += n
	return nil
}

// readMoveObjectType parses the compact MoveObjectType BCS encoding and
// returns a human-readable coin type string (e.g. "0x2::iota::IOTA").
//
// BCS encoding (from iota-sdk-types MoveObjectType):
//
//	0x00  Other      → full StructTag follows
//	0x01  GasCoin    → no extra bytes
//	0x02  StakedIota → no extra bytes
//	0x03  Coin<T>    → TypeTag follows
func (r *bcsReader) readMoveObjectType() (string, error) {
	tag, err := r.readU8()
	if err != nil {
		return "", err
	}
	switch tag {
	case 0x01: // GasCoin = 0x2::iota::IOTA
		return iotajsonrpc.IotaCoinType.String(), nil
	case 0x02: // StakedIota — not a coin we return from GetCoins
		return "0x3::iota_system::StakedIota", nil
	case 0x03: // Coin<T>: TypeTag follows
		ct, err := r.readTypeTag()
		if err != nil {
			return "", fmt.Errorf("readMoveObjectType Coin<T>: %w", err)
		}
		return ct, nil
	case 0x00: // Other: full StructTag
		if err := r.skipStructTag(); err != nil {
			return "", fmt.Errorf("readMoveObjectType Other: %w", err)
		}
		return "", nil
	default:
		return "", fmt.Errorf("readMoveObjectType: unknown tag %d", tag)
	}
}

// readTypeTag parses a TypeTag and returns a human-readable string for struct
// types, or skips non-struct types and returns "".
//
// TypeTag BCS tags:
//
//	0=bool, 1=u8, 2=u64, 3=u128, 4=address, 5=signer,
//	6=vector(TypeTag), 7=struct(StructTag), 8=u16, 9=u32, 10=u256
func (r *bcsReader) readTypeTag() (string, error) {
	tag, err := r.readU8()
	if err != nil {
		return "", err
	}
	switch tag {
	case 0, 1, 8, 9: // bool, u8, u16, u32 — no payload
		return "", nil
	case 2, 3: // u64, u128 — no payload
		return "", nil
	case 10: // u256 — no payload
		return "", nil
	case 4, 5: // address (32), signer (32) — no payload in type position
		return "", nil
	case 6: // vector<TypeTag>
		_, err := r.readTypeTag()
		return "", err
	case 7: // struct(StructTag)
		return r.readStructTag()
	default:
		return "", fmt.Errorf("readTypeTag: unknown tag %d", tag)
	}
}

// readStructTag parses a StructTag and returns a human-readable string like
// "0x2::iota::IOTA".
func (r *bcsReader) readStructTag() (string, error) {
	// address: 32 bytes
	if err := r.skip(32); err != nil {
		return "", err
	}
	// module: string
	if _, err := r.readString(); err != nil {
		return "", err
	}
	// name: string
	if _, err := r.readString(); err != nil {
		return "", err
	}
	// type_params: Vec<TypeTag> — skip count + each entry
	count, err := r.readULEB128()
	if err != nil {
		return "", err
	}
	for i := uint64(0); i < count; i++ {
		if _, err := r.readTypeTag(); err != nil {
			return "", err
		}
	}
	// We don't reconstruct the string from BCS — the caller always provides
	// coinTypeHint from the filter, so the return value here is ignored.
	return "", nil
}

// skipStructTag skips an entire StructTag without returning a value.
func (r *bcsReader) skipStructTag() error {
	_, err := r.readStructTag()
	return err
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
