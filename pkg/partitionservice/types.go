// Copyright 2021-2024 Matrix Origin
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

package partitionservice

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

var (
	PartitionTableMetadataSQL = fmt.Sprintf(`create table %s.%s(
		table_id 		           bigint        unsigned not null,  
		partition_method           varchar(13)            not null,  
		partition_id               bigint        unsigned not null,
		partition_ordinal_position int	         unsigned not null,
		partition_expression       varchar(2048)          not null,
		partition_description      text                   not null,
		partition_comment          text
	)`, catalog.MO_CATALOG, catalog.MOShardsMetadata)

	InitSQLs = []string{
		PartitionTableMetadataSQL,
	}
)

type PartitionMethod string

type Partition struct {
	// PartitionID we implement the partitioned table using a real physical table corresponding to
	// each partition given to the partitioned table. The ID of the partitioned table is the id of
	// the physical table.
	PartitionID uint64
	// PartitionMethod is the method of partition. (HASH, RANGE, LIST etc.)
	PartitionMethod PartitionMethod
}

// PartitionService is used to maintaining the metadata of the partition table.
type PartitionService interface {
	// Create creates metadata of the partition table.
	Create(
		ctx context.Context,
		tableID uint64,
		txnOp client.TxnOperator,
	) error
}
