// Copyright 2021 - 2024 Matrix Origin
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

package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCreateHashBased(t *testing.T) {
	runPartitionClusterTest(
		func(c embed.Cluster) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)

			db := testutils.GetDatabaseName(t)
			testutils.CreateTestDatabase(t, db, cn)

			testutils.ExecSQL(
				t,
				db,
				cn,
				fmt.Sprintf("create table %s (c int) partition by hash(c) partitions 2", t.Name()),
			)

			metadata := getMetadata(
				t,
				0,
				db,
				t.Name(),
				cn,
			)
			require.Equal(t, 2, len(metadata.Partitions))
		},
	)
}

func getMetadata(
	t *testing.T,
	accountID uint32,
	db string,
	table string,
	cn embed.ServiceOperator,
) partition.PartitionMetadata {
	ps := partitionservice.GetService(cn.ServiceID())
	store := ps.GetStorage()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = defines.AttachAccountId(ctx, accountID)

	var value partition.PartitionMetadata
	exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
	err := exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			id := testutils.MustGetTableID(
				t,
				db,
				table,
				txn,
			)

			metadata, ok, err := store.GetMetadata(
				ctx,
				id,
				txn.Txn(),
			)
			require.NoError(t, err)
			require.True(t, ok)
			value = metadata
			return nil
		},
		executor.Options{},
	)
	require.NoError(t, err)
	return value
}
