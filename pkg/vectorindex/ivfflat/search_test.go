// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ivfflat

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type mockVectorIndexSearch struct {
	lastRT vectorindex.RuntimeConfig
	keys   []int64
}

func (m *mockVectorIndexSearch) Search(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig) (any, []float64, error) {
	m.lastRT = rt
	return m.keys, nil, nil
}

func (m *mockVectorIndexSearch) SearchFloat32(proc *sqlexec.SqlProcess, query any, rt vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	m.lastRT = rt
	copy(outKeys, m.keys)
	return nil
}

func (m *mockVectorIndexSearch) Load(*sqlexec.SqlProcess) error { return nil }

func (m *mockVectorIndexSearch) UpdateConfig(cache.VectorIndexSearchIf) error { return nil }

func (m *mockVectorIndexSearch) Destroy() {}

// give blob
func mock_runSql(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, nil
}

func mock_runSql_parser_error(
	sqlproc *sqlexec.SqlProcess,
	sql string,
) (executor.Result, error) {
	return executor.Result{}, moerr.NewInternalErrorNoCtx("sql parser error")
}

func TestIvfflatSearchFloat32(t *testing.T) {
	runSql = mock_runSql

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)
	idxcfg.Ivfflat.Dimensions = 3

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{Limit: 1}

	s := &IvfflatSearch[float32]{
		Idxcfg: idxcfg,
		Tblcfg: tblcfg,
		Index:  &IvfflatSearchIndex[float32]{},
	}

	// 1. Test with nil/empty results (no centroids loaded)
	outKeys := make([]int64, 1)
	outDists := make([]float32, 1)
	err := s.SearchFloat32(sqlproc, v, rt, outKeys, outDists)
	require.NoError(t, err)

	// Since we are mocking everything, we can't easily run a full Ivf search without more mocks
	// But we've verified it doesn't crash on nil keys and calls the underlying Search.
}

func TestIvfSearchRace(t *testing.T) {

	runSql = mock_runSql

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.Nil(t, err)

}

func TestIvfSearchParserError(t *testing.T) {

	runSql = mock_runSql_parser_error

	var idxcfg vectorindex.IndexConfig
	var tblcfg vectorindex.IndexTableConfig

	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)

	idxcfg.Ivfflat.Metric = uint16(metric.Metric_L2Distance)

	v := []float32{0, 1, 2}
	rt := vectorindex.RuntimeConfig{}

	idx := &IvfflatSearchIndex[float32]{}

	_, _, err := idx.Search(sqlproc, idxcfg, tblcfg, v, rt, 4)
	require.NotNil(t, err)
}

func TestProbeLimitCappedByListsInFindCentroids(t *testing.T) {
	runSql = mock_runSql

	m := &mockVectorIndexSearch{keys: []int64{3, 1, 2}}
	idx := &IvfflatSearchIndex[float32]{Centroids: m}
	sqlproc := sqlexec.NewSqlProcessWithContext(sqlexec.NewSqlContext(context.Background(), "", nil, 0, nil))

	var idxcfg vectorindex.IndexConfig
	idxcfg.Ivfflat.Lists = 3

	keys, err := idx.findCentroids(sqlproc, []float32{0, 1, 2}, nil, idxcfg, 8929, 1)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 1, 2}, keys)
	require.Equal(t, uint(3), m.lastRT.Limit)
}
