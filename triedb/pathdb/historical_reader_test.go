// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"bytes"
	"errors"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/internal/testrand"
)

// TestHistoricalReaderStale verifies that HistoricalStateReader retries
// when the disk layer becomes stale during a read, rather than returning
// an error that StateDB would swallow (causing silent incorrect results).
//
// This is a regression test for issue #35963.
func TestHistoricalReaderStale(t *testing.T) {
	// Set maxDiffLayers low to force capping, ensuring old states are in history
	maxDiffLayers = 4
	defer func() {
		maxDiffLayers = 128
	}()

	// Create a database with state history and indexing enabled, with enough
	// layers that capping will commit and mark the bottom disk layer stale.
	tester := newTester(t, &testerConfig{
		stateHistory: 10,
		layers:       10,
		enableIndex:  true,
	})
	defer tester.release()

	// Wait for indexing to complete
	waitIndexing(tester.db)

	// Pick an old state to read historically (must be on disk)
	targetStateID := uint64(2)
	targetRoot := tester.roots[targetStateID]

	// Create the historical reader
	reader, err := tester.db.HistoricReader(targetRoot)
	if err != nil {
		t.Fatalf("Failed to create historic reader: %v", err)
	}

	// Pick an account and storage slot that exist in that state
	var (
		testAddr common.Address
		testSlot common.Hash
	)
	for addrHash := range tester.states[targetStateID].accountData {
		testAddr = tester.accountPreimage(addrHash)
		// Find a storage slot for this account
		if slots, ok := tester.states[targetStateID].storageData[addrHash]; ok && len(slots) > 0 {
			for slotHash := range slots {
				testSlot = tester.hashPreimage(slotHash)
				break
			}
		}
		break
	}

	if testAddr == (common.Address{}) {
		t.Fatal("No test account found in target state")
	}

	// Get the expected values for verification
	expectedAccount, err := reader.Account(testAddr)
	if err != nil {
		t.Fatalf("Failed to read account before commit: %v", err)
	}
	expectedStorage, err := reader.Storage(testAddr, testSlot)
	if err != nil {
		t.Fatalf("Failed to read storage before commit: %v", err)
	}

	// Now generate and commit enough new states to force a cap operation,
	// which will mark the old disk layer stale
	const extraLayers = 10
	var wg sync.WaitGroup
	var readErr error

	// Start a goroutine that will commit multiple layers
	wg.Add(1)
	go func() {
		defer wg.Done()
		tester.extend(extraLayers)
	}()

	// Concurrently, read the historical state many times
	// Some reads will hit the stale disk layer window
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// Create a fresh reader for each iteration to hit the race window
			r, err := tester.db.HistoricReader(targetRoot)
			if err != nil {
				readErr = err
				return
			}

			// Read account
			account, err := r.Account(testAddr)
			if err != nil {
				readErr = err
				return
			}
			if !accountsEqual(account, expectedAccount) {
				readErr = errors.New("account mismatch during concurrent commit")
				return
			}

			// Read storage
			storage, err := r.Storage(testAddr, testSlot)
			if err != nil {
				readErr = err
				return
			}
			if !bytes.Equal(storage, expectedStorage) {
				readErr = errors.New("storage mismatch during concurrent commit")
				return
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		t.Fatalf("Historical read failed during concurrent commits: %v", readErr)
	}
}

// TestHistoricalReaderIndexStale verifies that HistoricalStateReader retries
// when the history index is ahead of the disk layer (the second race window
// during commit, where history is indexed before the new disk layer is published).
func TestHistoricalReaderIndexStale(t *testing.T) {
	// Set maxDiffLayers low to force capping
	maxDiffLayers = 4
	defer func() {
		maxDiffLayers = 128
	}()

	// Create a database with state history and indexing enabled
	tester := newTester(t, &testerConfig{
		stateHistory: 10,
		layers:       10,
		enableIndex:  true,
	})
	defer tester.release()

	// Wait for indexing to complete
	waitIndexing(tester.db)

	// Pick an old state to read historically
	targetStateID := uint64(2)
	targetRoot := tester.roots[targetStateID]

	// Create the historical reader
	reader, err := tester.db.HistoricReader(targetRoot)
	if err != nil {
		t.Fatalf("Failed to create historic reader: %v", err)
	}

	// Pick an account that exists in that state
	var testAddr common.Address
	for addrHash := range tester.states[targetStateID].accountData {
		testAddr = tester.accountPreimage(addrHash)
		break
	}

	if testAddr == (common.Address{}) {
		t.Fatal("No test account found in target state")
	}

	// Get the expected value
	expectedAccount, err := reader.Account(testAddr)
	if err != nil {
		t.Fatalf("Failed to read account: %v", err)
	}

	// Generate more states, which will trigger indexing
	tester.extend(5)

	// Read again - should still work even if index got ahead during one of the commits
	reader, err = tester.db.HistoricReader(targetRoot)
	if err != nil {
		t.Fatalf("Failed to create historic reader after extend: %v", err)
	}

	account, err := reader.Account(testAddr)
	if err != nil {
		t.Fatalf("Failed to read account after extend: %v", err)
	}

	if !accountsEqual(account, expectedAccount) {
		t.Fatal("Account mismatch after extend")
	}
}

// TestHistoricalNodeReaderStale verifies that HistoricalNodeReader also retries
// correctly when the disk layer becomes stale.
func TestHistoricalNodeReaderStale(t *testing.T) {
	// Set maxDiffLayers low to force capping
	maxDiffLayers = 4
	defer func() {
		maxDiffLayers = 128
	}()

	// Create a database with trie node history
	tester := newTester(t, &testerConfig{
		stateHistory: 10,
		layers:       10,
		enableIndex:  true,
	})
	defer tester.release()

	// Wait for indexing to complete
	waitIndexing(tester.db)

	// Pick an old state
	targetStateID := uint64(2)
	targetRoot := tester.roots[targetStateID]

	// Create the historical node reader
	reader, err := tester.db.HistoricNodeReader(targetRoot)
	if err != nil {
		t.Fatalf("Failed to create historic node reader: %v", err)
	}

	// Read the state root node
	stateRootNode, err := reader.Node(common.Hash{}, nil, targetRoot)
	if err != nil {
		t.Fatalf("Failed to read state root node: %v", err)
	}

	// Extend to trigger commits
	var wg sync.WaitGroup
	var readErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		tester.extend(10)
	}()

	// Concurrently read the historical trie node
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			r, err := tester.db.HistoricNodeReader(targetRoot)
			if err != nil {
				readErr = err
				return
			}

			node, err := r.Node(common.Hash{}, nil, targetRoot)
			if err != nil {
				readErr = err
				return
			}
			if !bytes.Equal(node, stateRootNode) {
				readErr = errors.New("node mismatch during concurrent commit")
				return
			}
		}
	}()

	wg.Wait()

	if readErr != nil {
		t.Fatalf("Historical node read failed during concurrent commits: %v", readErr)
	}
}

// accountsEqual compares two StateAccount objects, handling nils correctly.
func accountsEqual(a, b *types.SlimAccount) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Nonce == b.Nonce &&
		a.Balance.Cmp(b.Balance) == 0 &&
		bytes.Equal(a.CodeHash, b.CodeHash) &&
		bytes.Equal(a.Root, b.Root)
}

// TestHistoricalReaderEmptyResults verifies that the retry logic doesn't
// break the case where an account or slot doesn't exist (should return nil/empty,
// not an error).
func TestHistoricalReaderEmptyResults(t *testing.T) {
	// Set maxDiffLayers low to force capping
	maxDiffLayers = 4
	defer func() {
		maxDiffLayers = 128
	}()

	tester := newTester(t, &testerConfig{
		stateHistory: 10,
		layers:       10,
		enableIndex:  true,
	})
	defer tester.release()

	// Wait for indexing to complete
	waitIndexing(tester.db)

	// Use an old state
	targetRoot := tester.roots[2]
	reader, err := tester.db.HistoricReader(targetRoot)
	if err != nil {
		t.Fatalf("Failed to create historic reader: %v", err)
	}

	// Read a non-existent account
	nonExistentAddr := testrand.Address()
	account, err := reader.Account(nonExistentAddr)
	if err != nil {
		t.Fatalf("Reading non-existent account should not error: %v", err)
	}
	if account != nil {
		t.Fatal("Non-existent account should return nil")
	}

	// Read a non-existent storage slot
	// First get an existing account
	var existingAddr common.Address
	for addrHash := range tester.states[2].accountData {
		existingAddr = tester.accountPreimage(addrHash)
		break
	}

	nonExistentSlot := testrand.Hash()
	storage, err := reader.Storage(existingAddr, nonExistentSlot)
	if err != nil {
		t.Fatalf("Reading non-existent storage should not error: %v", err)
	}
	if len(storage) != 0 {
		t.Fatal("Non-existent storage slot should return empty")
	}
}

// TestHistoricalReaderRealErrors verifies that real errors (not stale errors)
// are still properly returned and not retried forever.
func TestHistoricalReaderRealErrors(t *testing.T) {
	tester := newTester(t, &testerConfig{
		stateHistory: 5,
		layers:       5,
		enableIndex:  true,
	})
	defer tester.release()

	// Try to read from a state that doesn't exist
	// This should return an error, not retry forever
	fakeRoot := crypto.Keccak256Hash([]byte("nonexistent"))
	_, err := tester.db.HistoricReader(fakeRoot)
	if err == nil {
		t.Fatal("Reading nonexistent state should return error")
	}
	// The error could be "not available" or "haven't been fully indexed yet"
	// depending on indexing state, both are acceptable as long as it errors
}
