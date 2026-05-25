// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// Package iotagrpc: BCS type definitions for decoding gRPC node responses.
//
// The gRPC node wraps all objects in a VersionedObject BCS envelope
// (see iota-sdk-grpc-types/src/proto/iota/grpc/v1/versioned.rs).
// The inner Object layout follows iota-sdk-types/src/object.rs.
//
// NOTE: These types intentionally differ from iotago.MoveObject, which carries
// an extra HasPublicTransfer bool field used on the JSON-RPC path. That field
// is NOT present in the BCS wire format of iota-sdk-types::MoveStruct.
package iotagrpc

import (
	"fmt"

	bcs "github.com/iotaledger/bcs-go"
	"github.com/iotaledger/wasp/clients/iota-go/iotago"
)

// ── VersionedObject ───────────────────────────────────────────────────────────

// versionedObject mirrors the gRPC VersionedObject BCS envelope:
//
//	enum VersionedObject { V1(Object) }
type versionedObject struct {
	V1 *grpcObject // variant 0
}

func (versionedObject) IsBcsEnum() {}

// ── Object ────────────────────────────────────────────────────────────────────

// grpcObject mirrors iota-sdk-types Object:
//
//	struct Object { data: ObjectData, owner: Owner, previous_transaction: Digest, storage_rebate: u64 }
type grpcObject struct {
	Data                grpcObjectData
	Owner               iotago.Owner
	PreviousTransaction [32]byte // Digest — not used directly; callers use the reference field
	StorageRebate       uint64
}

// ── ObjectData ────────────────────────────────────────────────────────────────

// grpcObjectData mirrors iota-sdk-types ObjectData:
//
//	enum ObjectData { Struct(MoveStruct) = 0, Package(MovePackage) = 1 }
//
// We implement UnmarshalBCS directly to avoid having to define the full
// MovePackage type — packages are never used in our gRPC call sites.
type grpcObjectData struct {
	Struct *grpcMoveStruct // variant 0; nil if object is a package
}

func (d *grpcObjectData) UnmarshalBCS(dec *bcs.Decoder) error {
	tag := dec.ReadEnumIdx()
	if dec.Err() != nil {
		return fmt.Errorf("grpcObjectData: read variant tag: %w", dec.Err())
	}
	switch tag {
	case 0: // Struct
		var ms grpcMoveStruct
		dec.Decode(&ms)
		if dec.Err() != nil {
			return fmt.Errorf("grpcObjectData: decode MoveStruct: %w", dec.Err())
		}
		d.Struct = &ms
	case 1: // Package — not needed, skip is not possible without full type definition
		return fmt.Errorf("grpcObjectData: MovePackage objects are not supported in this context")
	default:
		return fmt.Errorf("grpcObjectData: unknown variant tag %d", tag)
	}
	return nil
}

// ── MoveStruct ────────────────────────────────────────────────────────────────

// grpcMoveStruct mirrors iota-sdk-types MoveStruct:
//
//	struct MoveStruct { object_type: MoveObjectType, version: u64, contents: Vec<u8> }
//
// Intentionally has NO HasPublicTransfer field (unlike iotago.MoveObject).
type grpcMoveStruct struct {
	ObjectType iotago.MoveObjectType
	Version    uint64
	Contents   []byte
}

// ── Owner helpers ─────────────────────────────────────────────────────────────

// ownerAddress returns the owner address for AddressOwner and ObjectOwner,
// nil for Shared and Immutable.
func ownerAddress(o iotago.Owner) *iotago.Address {
	if o.AddressOwner != nil {
		return o.AddressOwner
	}
	if o.ObjectOwner != nil {
		return o.ObjectOwner
	}
	return nil
}

// ── Coin contents ─────────────────────────────────────────────────────────────

// coinContents mirrors the Move Coin<T> struct layout in BCS:
//
//	struct Coin<T> { id: UID, balance: Balance }
//
// UID = { id: ID } = { bytes: ObjectID } = [32]byte (transparent encoding)
// Balance = { value: u64 }
//
// Total: 32 bytes object-id + 8 bytes u64 balance.
type coinContents struct {
	ID      [32]byte // UID → ID → ObjectID (all transparent, encodes as raw 32 bytes)
	Balance uint64   // Balance { value: u64 } (single field, same encoding)
}

// ── MoveObjectType → coin type string ─────────────────────────────────────────

// moveObjectTypeToCoinType converts a decoded MoveObjectType to a human-readable
// coin type string. Returns "" for non-coin types (Other, StakedIota).
func moveObjectTypeToCoinType(t iotago.MoveObjectType) string {
	switch {
	case t.GasCoin != nil:
		return "0x2::iota::IOTA"
	case t.Iota != nil:
		// StakedIota — not a transferable coin
		return ""
	case t.Coin != nil:
		return t.Coin.String()
	default:
		// Other(StructTag) — not a standard coin
		return ""
	}
}

