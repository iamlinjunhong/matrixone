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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
)

type service struct {
	sid   string
	store PartitionStorage
}

func NewService(
	sid string,
	store PartitionStorage,
) PartitionService {
	return &service{
		sid:   sid,
		store: store,
	}
}

func (s *service) Create(
	ctx context.Context,
	tableID uint64,
	option *tree.PartitionOption,
	txnOp client.TxnOperator,
) error {
	def, err := s.store.GetTableDef(
		ctx,
		tableID,
		txnOp,
	)
	if err != nil {
		return err
	}

	metadata, err := s.getMetadata(
		def,
		option,
	)
	if err != nil {
		return err
	}

	for _, p := range metadata.Partitions {
		if err := s.store.Create(
			ctx,
			def,
			metadata,
			p,
			txnOp,
		); err != nil {
			return err
		}
	}

	return nil
}

func (s *service) getMetadata(
	def *plan.TableDef,
	option *tree.PartitionOption,
) (partition.PartitionMetadata, error) {
	if option == nil || option.PartBy == nil {
		panic("BUG: partition option is nil")
	}
	if option.PartBy.IsSubPartition {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("sub-partition is not supported")
	}

	method := option.PartBy.PType
	switch method.(type) {
	case *tree.HashType:
		return s.getMetadataByHashType(
			option,
			def,
		)
	default:
		panic("BUG: unsupported partition method")
	}

}

func (s *service) getMetadataByHashType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.HashType)
	if option.PartBy.Num <= 0 {
		return partition.PartitionMetadata{}, moerr.NewInvalidInputNoCtx("partition number is invalid")
	}

	columns, ok := method.Expr.(*tree.UnresolvedName)
	if !ok {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("column expression is not supported")
	}
	if columns.NumParts != 1 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in HASH partition")
	}
	validColumns, err := validColumns(
		columns,
		def,
		func(t plan.Type) bool {
			return types.T(t.Id).IsInteger()
		},
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}

	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteString(true),
	)
	method.Expr.Format(ctx)

	metadata := partition.PartitionMetadata{
		ID:          def.TblId,
		Columns:     validColumns,
		Description: ctx.String(),
		Method:      partition.PartitionMethod_Hash,
	}

	for i := uint64(0); i < option.PartBy.Num; i++ {
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:     fmt.Sprintf("p%d", i),
				Position: uint32(i),
				// TODO: ???
				Expression: "",
				Comment:    "",
			},
		)
	}
	return metadata, nil
}

func (s *service) getMetadataByKeyType(
	option *tree.PartitionOption,
	def *plan.TableDef,
) (partition.PartitionMetadata, error) {
	method := option.PartBy.PType.(*tree.HashType)
	if option.PartBy.Num <= 0 {
		return partition.PartitionMetadata{}, moerr.NewInvalidInputNoCtx("partition number is invalid")
	}

	columns, ok := method.Expr.(*tree.UnresolvedName)
	if !ok {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("column expression is not supported")
	}
	if columns.NumParts != 1 {
		return partition.PartitionMetadata{}, moerr.NewNotSupportedNoCtx("multi-column is not supported in HASH partition")
	}
	validColumns, err := validColumns(
		columns,
		def,
		func(t plan.Type) bool {
			return types.T(t.Id).IsInteger()
		},
	)
	if err != nil {
		return partition.PartitionMetadata{}, err
	}

	ctx := tree.NewFmtCtx(
		dialect.MYSQL,
		tree.WithQuoteString(true),
	)
	method.Expr.Format(ctx)

	metadata := partition.PartitionMetadata{
		ID:          def.TblId,
		Columns:     validColumns,
		Description: ctx.String(),
		Method:      partition.PartitionMethod_Hash,
	}

	for i := uint64(0); i < option.PartBy.Num; i++ {
		metadata.Partitions = append(
			metadata.Partitions,
			partition.Partition{
				Name:     fmt.Sprintf("p%d", i),
				Position: uint32(i),
				// TODO: ???
				Expression: "",
				Comment:    "",
			},
		)
	}
	return metadata, nil
}

func validColumns(
	columns *tree.UnresolvedName,
	tableDefine *plan.TableDef,
	validType func(plan.Type) bool,
) ([]string, error) {
	validColumns := make([]string, 0, columns.NumParts)
	for i := 0; i < columns.NumParts; i++ {
		v := columns.CStrParts[i]
		col := v.Compare()
		has := false
		for _, c := range tableDefine.GetCols() {
			if !strings.EqualFold(c.Name, col) {
				continue
			}

			has = true
			if !validType(c.Typ) {
				return nil, moerr.NewNotSupportedNoCtx("column type is not supported in hash partition")
			}
			break
		}
		if !has {
			return nil, moerr.NewErrWrongColumnName(moerr.Context(), v.Origin())
		}
		validColumns = append(validColumns, col)
	}
	return validColumns, nil
}
