// Copyright 2026 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package txnsnapshot

import (
	"math"
	"sync"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
)

func newSnapshotWithRuntimeStats(stats *SnapshotRuntimeStats) *KVSnapshot {
	snapshot := &KVSnapshot{}
	snapshot.SetRuntimeStats(stats)
	return snapshot
}

func TestSnapshotRuntimeStatsGetScanDetailAndCoverage(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)

	detail, detailRecords, completedResponses := stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{}, detail)
	require.Zero(t, detailRecords)
	require.Zero(t, completedResponses)

	// A missing ExecDetailsV2 is different from a present, zero-valued
	// ScanDetailV2 record.
	snapshot.mergeExecDetail(nil)
	detail, detailRecords, completedResponses = stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{}, detail)
	require.Zero(t, detailRecords)
	require.Equal(t, uint64(1), completedResponses)

	snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{})
	detail, detailRecords, completedResponses = stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{}, detail)
	require.Zero(t, detailRecords)
	require.Equal(t, uint64(2), completedResponses)

	snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}})
	detail, detailRecords, completedResponses = stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{}, detail)
	require.Equal(t, uint64(1), detailRecords)
	require.Equal(t, uint64(3), completedResponses)

	snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:            11,
		ProcessedVersions:        7,
		ProcessedVersionsSize:    70,
		RocksdbBlockReadByte:     99,
		IaRemoteReadSegmentBytes: 101,
	}})
	detail, detailRecords, completedResponses = stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{
		TotalKeys:         11,
		ProcessedKeys:     7,
		ProcessedKeysSize: 70,
	}, detail)
	require.Equal(t, uint64(2), detailRecords)
	require.Equal(t, uint64(4), completedResponses)

	// The accessor returns an independent value and exposes only the three
	// fields in its contract.
	detail.TotalKeys = 1000
	detail, _, _ = stats.GetScanDetailAndCoverage()
	require.Equal(t, int64(11), detail.TotalKeys)
	require.Zero(t, detail.RocksdbBlockReadByte)
}

func TestSnapshotRuntimeStatsScanDetailCloneAndMerge(t *testing.T) {
	source := &SnapshotRuntimeStats{}
	sourceSnapshot := newSnapshotWithRuntimeStats(source)
	sourceSnapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         5,
		ProcessedVersions:     3,
		ProcessedVersionsSize: 30,
	}})
	sourceSnapshot.mergeExecDetail(nil)

	clone := source.Clone()
	sourceSnapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
		TotalVersions:         7,
		ProcessedVersions:     4,
		ProcessedVersionsSize: 40,
	}})

	detail, detailRecords, completedResponses := clone.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{
		TotalKeys:         5,
		ProcessedKeys:     3,
		ProcessedKeysSize: 30,
	}, detail)
	require.Equal(t, uint64(1), detailRecords)
	require.Equal(t, uint64(2), completedResponses)

	target := &SnapshotRuntimeStats{}
	target.Merge(clone)
	target.Merge(source)
	detail, detailRecords, completedResponses = target.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{
		TotalKeys:         17,
		ProcessedKeys:     10,
		ProcessedKeysSize: 100,
	}, detail)
	require.Equal(t, uint64(3), detailRecords)
	require.Equal(t, uint64(5), completedResponses)
}

func TestSnapshotRuntimeStatsScanDetailInvalidSentinel(t *testing.T) {
	requireInvalid := func(t *testing.T, stats *SnapshotRuntimeStats) {
		detail, detailRecords, completedResponses := stats.GetScanDetailAndCoverage()
		require.Equal(t, util.ScanDetail{
			TotalKeys:         -1,
			ProcessedKeys:     -1,
			ProcessedKeysSize: -1,
		}, detail)
		require.Zero(t, detailRecords)
		require.Zero(t, completedResponses)
	}

	t.Run("nil receiver", func(t *testing.T) {
		var stats *SnapshotRuntimeStats
		requireInvalid(t, stats)
	})

	t.Run("protobuf uint64 conversion", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			detail *kvrpcpb.ScanDetailV2
		}{
			{name: "total versions", detail: &kvrpcpb.ScanDetailV2{TotalVersions: math.MaxUint64}},
			{name: "processed versions", detail: &kvrpcpb.ScanDetailV2{ProcessedVersions: math.MaxUint64}},
			{name: "processed versions size", detail: &kvrpcpb.ScanDetailV2{ProcessedVersionsSize: math.MaxUint64}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				stats := &SnapshotRuntimeStats{}
				newSnapshotWithRuntimeStats(stats).mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: tc.detail})
				requireInvalid(t, stats)
			})
		}
	})

	t.Run("single stats multiple responses", func(t *testing.T) {
		stats := &SnapshotRuntimeStats{}
		snapshot := newSnapshotWithRuntimeStats(stats)
		snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions:         math.MaxInt64,
			ProcessedVersions:     math.MaxInt64,
			ProcessedVersionsSize: math.MaxInt64,
			RocksdbBlockReadByte:  1,
		}})
		snapshot.mergeExecDetail(&kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions:         1,
			ProcessedVersions:     1,
			ProcessedVersionsSize: 1,
			RocksdbBlockReadByte:  2,
		}})
		requireInvalid(t, stats)
		internalDetail, _, _, invalid := stats.scanDetailSnapshot()
		require.True(t, invalid)
		require.Equal(t, uint64(3), internalDetail.RocksdbBlockReadByte)
		requireInvalid(t, stats.Clone())
	})

	t.Run("merge detail overflow", func(t *testing.T) {
		target := &SnapshotRuntimeStats{}
		newSnapshotWithRuntimeStats(target).mergeExecDetail(&kvrpcpb.ExecDetailsV2{
			ScanDetailV2: &kvrpcpb.ScanDetailV2{TotalVersions: math.MaxInt64, RocksdbBlockReadByte: 1},
		})
		source := &SnapshotRuntimeStats{}
		newSnapshotWithRuntimeStats(source).mergeExecDetail(&kvrpcpb.ExecDetailsV2{
			ScanDetailV2: &kvrpcpb.ScanDetailV2{TotalVersions: 1, RocksdbBlockReadByte: 2},
		})
		target.Merge(source)
		requireInvalid(t, target)
		internalDetail, _, _, invalid := target.scanDetailSnapshot()
		require.True(t, invalid)
		require.Equal(t, uint64(3), internalDetail.RocksdbBlockReadByte)
	})

	t.Run("record coverage overflow", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			stats  *SnapshotRuntimeStats
			detail *kvrpcpb.ExecDetailsV2
		}{
			{name: "completed responses", stats: &SnapshotRuntimeStats{completedResponses: math.MaxUint64}},
			{
				name: "detail records", stats: &SnapshotRuntimeStats{detailRecords: math.MaxUint64},
				detail: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				newSnapshotWithRuntimeStats(tc.stats).mergeExecDetail(tc.detail)
				requireInvalid(t, tc.stats)
			})
		}
	})

	t.Run("merge coverage overflow", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			target *SnapshotRuntimeStats
			source *SnapshotRuntimeStats
		}{
			{
				name:   "completed responses",
				target: &SnapshotRuntimeStats{completedResponses: math.MaxUint64},
				source: &SnapshotRuntimeStats{completedResponses: 1},
			},
			{
				name:   "detail records",
				target: &SnapshotRuntimeStats{detailRecords: math.MaxUint64},
				source: &SnapshotRuntimeStats{detailRecords: 1},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tc.target.Merge(tc.source)
				requireInvalid(t, tc.target)
			})
		}
	})
}

func TestCollectBatchGetResponseDataScanDetailCoverage(t *testing.T) {
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	collect := func(resp any) (*batchGetLockInfo, error) {
		return collectBatchGetResponseData(
			&tikvrpc.Response{Resp: resp},
			func([]byte, kv.ValueEntry) {},
			snapshot.mergeExecDetail,
		)
	}

	_, err := collectBatchGetResponseData(
		&tikvrpc.Response{},
		func([]byte, kv.ValueEntry) {},
		snapshot.mergeExecDetail,
	)
	require.Error(t, err)

	_, err = collect(&kvrpcpb.BatchGetResponse{})
	require.NoError(t, err)
	_, err = collect(&kvrpcpb.BatchGetResponse{
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{}},
	})
	require.NoError(t, err)

	lockInfo, err := collect(&kvrpcpb.BatchGetResponse{
		Error: &kvrpcpb.KeyError{Locked: &kvrpcpb.LockInfo{
			PrimaryLock: []byte("k"),
			LockVersion: 1,
			Key:         []byte("k"),
			LockTtl:     1,
			TxnSize:     1,
			LockType:    kvrpcpb.Op_Put,
		}},
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions:         2,
			ProcessedVersions:     1,
			ProcessedVersionsSize: 10,
		}},
	})
	require.NoError(t, err)
	require.Len(t, lockInfo.lockedKeys, 1)

	// The successful retry is a separate completed response because its scan
	// detail is also a separate record in the aggregate.
	_, err = collect(&kvrpcpb.BatchGetResponse{
		ExecDetailsV2: &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
			TotalVersions:         3,
			ProcessedVersions:     2,
			ProcessedVersionsSize: 20,
		}},
	})
	require.NoError(t, err)
	_, err = collect(&kvrpcpb.BufferBatchGetResponse{})
	require.NoError(t, err)

	_, err = collect(&kvrpcpb.GetResponse{})
	require.Error(t, err)

	detail, detailRecords, completedResponses := stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{
		TotalKeys:         5,
		ProcessedKeys:     3,
		ProcessedKeysSize: 30,
	}, detail)
	require.Equal(t, uint64(3), detailRecords)
	require.Equal(t, uint64(5), completedResponses)
}

func TestSnapshotRuntimeStatsConcurrentScanDetailAccess(t *testing.T) {
	const (
		workers       = 8
		responsesEach = 100
	)
	stats := &SnapshotRuntimeStats{}
	snapshot := newSnapshotWithRuntimeStats(stats)
	done := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
				stats.GetScanDetailAndCoverage()
				clone := stats.Clone()
				merged := &SnapshotRuntimeStats{}
				merged.Merge(clone)
				_ = merged.String()
			}
		}
	}()

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range responsesEach {
				response := &kvrpcpb.BatchGetResponse{}
				if i%2 == 0 {
					response.ExecDetailsV2 = &kvrpcpb.ExecDetailsV2{ScanDetailV2: &kvrpcpb.ScanDetailV2{
						TotalVersions:         1,
						ProcessedVersions:     2,
						ProcessedVersionsSize: 3,
					}}
				} else {
					response.ExecDetailsV2 = &kvrpcpb.ExecDetailsV2{}
				}
				_, err := collectBatchGetResponseData(
					&tikvrpc.Response{Resp: response},
					func([]byte, kv.ValueEntry) {},
					snapshot.mergeExecDetail,
				)
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	close(done)
	<-readerDone

	detail, detailRecords, completedResponses := stats.GetScanDetailAndCoverage()
	require.Equal(t, util.ScanDetail{
		TotalKeys:         workers * responsesEach / 2,
		ProcessedKeys:     workers * responsesEach,
		ProcessedKeysSize: workers * responsesEach * 3 / 2,
	}, detail)
	require.Equal(t, uint64(workers*responsesEach/2), detailRecords)
	require.Equal(t, uint64(workers*responsesEach), completedResponses)
}
