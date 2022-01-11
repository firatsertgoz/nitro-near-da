//
// Copyright 2021, Offchain Labs, Inc. All rights reserved.
//

package merkleAccumulator

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/offchainlabs/arbstate/arbos/storage"
	util_math "github.com/offchainlabs/arbstate/util"
)

type MerkleAccumulator struct {
	backingStorage *storage.Storage
	size           storage.WrappedUint64
	partials       []*common.Hash // nil if we are using backingStorage (in that case we access partials in backingStorage
}

func InitializeMerkleAccumulator(sto *storage.Storage) {
	// no initialization needed
}

func OpenMerkleAccumulator(sto *storage.Storage) *MerkleAccumulator {
	size := sto.OpenStorageBackedUint64(0)
	return &MerkleAccumulator{sto, &size, nil}
}

func NewNonpersistentMerkleAccumulator() *MerkleAccumulator {
	return &MerkleAccumulator{nil, &storage.MemoryBackedUint64{}, make([]*common.Hash, 0)}
}

func CalcNumPartials(size uint64) uint64 {
	return util_math.Log2ceil(size)
}

func NewNonpersistentMerkleAccumulatorFromPartials(partials []*common.Hash) (*MerkleAccumulator, error) {
	size := uint64(0)
	levelSize := uint64(1)
	for i := range partials {
		if *partials[i] != (common.Hash{}) {
			size += levelSize
		}
		levelSize *= 2
	}
	mbu := &storage.MemoryBackedUint64{}
	return &MerkleAccumulator{nil, mbu, partials}, mbu.Set(size)
}

func (acc *MerkleAccumulator) NonPersistentClone() (*MerkleAccumulator, error) {
	size, err := acc.size.Get()
	if err != nil {
		return nil, err
	}
	numPartials := CalcNumPartials(size)
	partials := make([]*common.Hash, numPartials)
	for i := uint64(0); i < numPartials; i++ {
		partial, err := acc.getPartial(i)
		if err != nil {
			return nil, err
		}
		partials[i] = partial
	}
	mbu := &storage.MemoryBackedUint64{}
	return &MerkleAccumulator{nil, mbu, partials}, mbu.Set(size)
}

func (acc *MerkleAccumulator) getPartial(level uint64) (*common.Hash, error) {
	if acc.backingStorage == nil {
		if acc.partials[level] == nil {
			h := common.Hash{}
			acc.partials[level] = &h
		}
		return acc.partials[level], nil
	} else {
		ret, err := acc.backingStorage.GetByUint64(2 + level)
		return &ret, err
	}
}

func (acc *MerkleAccumulator) GetPartials() ([]*common.Hash, error) {
	size, err := acc.size.Get()
	if err != nil {
		return nil, err
	}
	partials := make([]*common.Hash, CalcNumPartials(size))
	for i := range partials {
		p, err := acc.getPartial(uint64(i))
		if err != nil {
			return nil, err
		}
		partials[i] = p
	}
	return partials, nil
}

func (acc *MerkleAccumulator) setPartial(level uint64, val *common.Hash) error {
	if acc.backingStorage != nil {
		err := acc.backingStorage.SetByUint64(2+level, *val)
		if err != nil {
			return err
		}
	} else if level == uint64(len(acc.partials)) {
		acc.partials = append(acc.partials, val)
	} else {
		acc.partials[level] = val
	}
	return nil
}

func (acc *MerkleAccumulator) Append(itemHash common.Hash) ([]MerkleTreeNodeEvent, error) {
	size, err := acc.size.Increment()
	if err != nil {
		return nil, err
	}
	events := []MerkleTreeNodeEvent{}

	level := uint64(0)
	soFar := itemHash.Bytes()
	for {
		if level == CalcNumPartials(size-1) { // -1 to counteract the acc.size++ at top of this function
			h := common.BytesToHash(soFar)
			err := acc.setPartial(level, &h)
			if err != nil {
				return nil, err
			}
			return events, nil
		}
		thisLevel, err := acc.getPartial(level)
		if err != nil {
			return nil, err
		}
		if *thisLevel == (common.Hash{}) {
			h := common.BytesToHash(soFar)
			err := acc.setPartial(level, &h)
			if err != nil {
				return nil, err
			}
			return events, nil
		}
		soFar = crypto.Keccak256(thisLevel.Bytes(), soFar)
		h := common.Hash{}
		err = acc.setPartial(level, &h)
		if err != nil {
			return nil, err
		}
		level += 1
		events = append(events, MerkleTreeNodeEvent{level, size - 1, common.BytesToHash(soFar)})
	}
}

func (acc *MerkleAccumulator) Size() (uint64, error) {
	return acc.size.Get()
}

func (acc *MerkleAccumulator) Root() (common.Hash, error) {
	size, err := acc.size.Get()
	if size == 0 || err != nil {
		return common.Hash{}, err
	}

	var hashSoFar *common.Hash
	var capacityInHash uint64
	capacity := uint64(1)
	for level := uint64(0); level < CalcNumPartials(size); level++ {
		partial, err := acc.getPartial(level)
		if err != nil {
			return common.Hash{}, err
		}
		if *partial != (common.Hash{}) {
			if hashSoFar == nil {
				hashSoFar = partial
				capacityInHash = capacity
			} else {
				for capacityInHash < capacity {
					h := crypto.Keccak256Hash(hashSoFar.Bytes(), make([]byte, 32))
					hashSoFar = &h
					capacityInHash *= 2
				}
				h := crypto.Keccak256Hash(partial.Bytes(), hashSoFar.Bytes())
				hashSoFar = &h
				capacityInHash = 2 * capacity
			}
		}
		capacity *= 2
	}
	return *hashSoFar, nil
}

func (acc *MerkleAccumulator) StateForExport() (uint64, common.Hash, []common.Hash, error) {
	root, err := acc.Root()
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	size, err := acc.size.Get()
	if err != nil {
		return 0, common.Hash{}, nil, err
	}
	numPartials := CalcNumPartials(size)
	partials := make([]common.Hash, numPartials)
	for i := uint64(0); i < numPartials; i++ {
		partial, err := acc.getPartial(i)
		if err != nil {
			return 0, common.Hash{}, nil, err
		}
		partials[i] = *partial
	}
	return size, root, partials, nil
}

type MerkleTreeNodeEvent struct {
	Level     uint64
	NumLeaves uint64
	Hash      common.Hash
}
