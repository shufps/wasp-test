// Copyright 2020 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

// BCS type definitions for decoding IotaSystemState from gRPC GetEpochInfo responses.
//
// These types mirror the Rust structs in
// crates/iota-types/src/iota_system_state/iota_system_state_inner_v{1,2}.rs.
//
// Only the fields up to and including the first field of SystemParametersV1
// (epoch_duration_ms) are decoded. Remaining bytes are left in the reader
// (streaming decode — no trailing-bytes check).
package iotagrpc

// ── ValidatorMetadataV1 ───────────────────────────────────────────────────────

type sysValidatorMetadataV1 struct {
	IotaAddress                   [32]byte
	AuthorityPubkeyBytes          []byte
	NetworkPubkeyBytes            []byte
	ProtocolPubkeyBytes           []byte
	ProofOfPossessionBytes        []byte
	Name                          string
	Description                   string
	ImageURL                      string
	ProjectURL                    string
	NetAddress                    string
	P2pAddress                    string
	PrimaryAddress                string
	NextEpochAuthorityPubkeyBytes *[]byte  `bcs:"optional"`
	NextEpochProofOfPossession    *[]byte  `bcs:"optional"`
	NextEpochNetworkPubkeyBytes   *[]byte  `bcs:"optional"`
	NextEpochProtocolPubkeyBytes  *[]byte  `bcs:"optional"`
	NextEpochNetAddress           *string  `bcs:"optional"`
	NextEpochP2pAddress           *string  `bcs:"optional"`
	NextEpochPrimaryAddress       *string  `bcs:"optional"`
	ExtraFields                   [40]byte // Bag { id: UID(32), size: u64(8) }
}

// ── StakingPoolV1 ─────────────────────────────────────────────────────────────

type sysStakingPoolV1 struct {
	ID                       [32]byte // ObjectID
	ActivationEpoch          *uint64  `bcs:"optional"` // Option<u64>
	DeactivationEpoch        *uint64  `bcs:"optional"` // Option<u64>
	IotaBalance              uint64
	RewardsPool              uint64   // Balance { value: u64 }
	PoolTokenBalance         uint64
	ExchangeRates            [40]byte // Table { id: ObjectID(32), size: u64(8) }
	PendingStake             uint64
	PendingTotalIotaWithdraw uint64
	PendingPoolTokenWithdraw uint64
	ExtraFields              [40]byte // Bag
}

// ── ValidatorV1 ───────────────────────────────────────────────────────────────

type sysValidatorV1 struct {
	Metadata                sysValidatorMetadataV1
	VotingPower             uint64
	OperationCapID          [32]byte // ID = { bytes: [u8; 32] }
	GasPrice                uint64
	StakingPool             sysStakingPoolV1
	CommissionRate          uint64
	NextEpochStake          uint64
	NextEpochGasPrice       uint64
	NextEpochCommissionRate uint64
	ExtraFields             [40]byte // Bag
}

// ── AtRiskEntry ───────────────────────────────────────────────────────────────

// sysAtRiskEntry mirrors VecMap<IotaAddress, u64> entry:
//
//	struct Entry<K, V> { key: K, value: V }
type sysAtRiskEntry struct {
	Key   [32]byte // IotaAddress
	Value uint64
}

// ── ValidatorSetV1 ────────────────────────────────────────────────────────────

type sysValidatorSetV1 struct {
	TotalStake              uint64
	ActiveValidators        []sysValidatorV1
	PendingActiveValidators [40]byte         // TableVec { contents: Table(40) }
	PendingRemovals         []uint64
	StakingPoolMappings     [40]byte         // Table
	InactiveValidators      [40]byte         // Table
	ValidatorCandidates     [40]byte         // Table
	AtRiskValidators        []sysAtRiskEntry // VecMap<IotaAddress, u64>
	ExtraFields             [40]byte         // Bag
}

// ── ValidatorSetV2 ────────────────────────────────────────────────────────────

type sysValidatorSetV2 struct {
	TotalStake              uint64
	ActiveValidators        []sysValidatorV1
	CommitteeMembers        []uint64         // NEW in V2
	PendingActiveValidators [40]byte         // TableVec
	PendingRemovals         []uint64
	StakingPoolMappings     [40]byte         // Table
	InactiveValidators      [40]byte         // Table
	ValidatorCandidates     [40]byte         // Table
	AtRiskValidators        []sysAtRiskEntry // VecMap<IotaAddress, u64>
	ExtraFields             [40]byte         // Bag
}

// ── Partial IotaSystemState heads ────────────────────────────────────────────
//
// These structs decode only the fields needed: epoch, protocol_version,
// system_state_version, and epoch_duration_ms (first field of SystemParametersV1).
// Fields after EpochDurationMs are left unread (streaming decoder).

type sysStateHeadV1 struct {
	Epoch              uint64
	ProtocolVersion    uint64
	SystemStateVersion uint64
	TreasuryCap        [40]byte         // IotaTreasuryCap (UID(32) + Supply.value(8))
	Validators         sysValidatorSetV1
	StorageFund        [16]byte         // StorageFundV1: 2 × Balance { value: u64 }
	EpochDurationMs    uint64           // first field of SystemParametersV1
}

type sysStateHeadV2 struct {
	Epoch              uint64
	ProtocolVersion    uint64
	SystemStateVersion uint64
	TreasuryCap        [40]byte         // IotaTreasuryCap
	Validators         sysValidatorSetV2
	StorageFund        [16]byte
	EpochDurationMs    uint64
}
