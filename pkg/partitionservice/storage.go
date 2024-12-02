package partitionservice

import (
	"context"
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/partition"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type storage struct {
	exec executor.SQLExecutor
	eng  engine.Engine
}

func (s *storage) GetTableDef(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (*plan.TableDef, error) {
	_, _, rel, err := s.eng.GetRelationById(
		ctx,
		txnOp,
		tableID,
	)
	if err != nil {
		return nil, err
	}
	return rel.GetTableDef(ctx), nil
}

func (s *storage) Create(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			partitionName, err := s.createPartitionTable(
				def,
				metadata,
				partition,
				txn,
			)
			if err != nil {
				return err
			}

			partitionID, err := s.getTableIDByTableNameAndDatabaseName(
				partitionName,
				def.DbName,
				txn,
			)
			if err != nil {
				return err
			}
			partition.PartitionID = partitionID

			return s.createPartitionMetadata(
				def,
				metadata,
				partition,
				txn,
			)
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithAccountID(accountID),
	)
}

func (s *storage) Delete(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) error {
	return nil
}

func (s *storage) createPartitionTable(
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
	txn executor.TxnExecutor,
) (string, error) {
	txn.Use(def.DbName)

	name, sql := getPartitionTableCreateSQL(
		def,
		metadata,
		partition,
	)

	res, err := txn.Exec(
		sql,
		executor.StatementOption{},
	)
	if err != nil {
		return "", err
	}
	res.Close()

	return name, nil
}

func (s *storage) getTableIDByTableNameAndDatabaseName(
	tableName string,
	databaseName string,
	txn executor.TxnExecutor,
) (uint64, error) {
	txn.Use(catalog.MO_CATALOG)

	res, err := txn.Exec(
		fmt.Sprintf("select rel_id from mo_catalog.mo_tables where relname = '%s' and reldatabase = '%s'",
			strings.ToLower(tableName),
			strings.ToLower(databaseName),
		),
		executor.StatementOption{},
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	id := uint64(0)
	res.ReadRows(
		func(rows int, cols []*vector.Vector) bool {
			id = executor.GetFixedRows[uint64](cols[0])[0]
			return false
		},
	)
	return id, nil
}

func (s *storage) createPartitionMetadata(
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
	txn executor.TxnExecutor,
) error {
	txn.Use(catalog.MO_CATALOG)

	sql := getInsertMetadataSQL(
		def,
		metadata,
		partition,
	)

	res, err := txn.Exec(
		sql,
		executor.StatementOption{},
	)
	if err != nil {
		return err
	}

	res.Close()
	return nil
}

func getPartitionTableCreateSQL(
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
) (string, string) {
	return "TODO", "TODO"
}

func getInsertMetadataSQL(
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
) string {
	// table_id 		           bigint        unsigned not null,
	// partition_method           varchar(13)            not null,
	// partition_id               bigint        unsigned not null,
	// partition_ordinal_position int	         unsigned not null,
	// partition_expression       varchar(2048)          not null,
	// partition_description      text                   not null,
	// partition_comment          text
	return fmt.Sprintf("insert into %s.%s values(%d, '%s', %d, %d, '%s', '%s', '%s')",
		catalog.MO_CATALOG,
		catalog.MOPartitionMetadata,
		def.TblId,
		metadata.Method.String(),
		partition.PartitionID,
		partition.Position,
		partition.Expression,
		metadata.Description,
		"",
	)
}
