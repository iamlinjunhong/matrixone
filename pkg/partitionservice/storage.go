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

func (s *storage) GetMetadata(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) (partition.PartitionMetadata, bool, error) {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return partition.PartitionMetadata{}, false, err
	}

	res, err := s.exec.Exec(
		ctx,
		`select 
			partition_method,
			partition_id,
			partition_ordinal_position,
			partition_expression,
			partition_description,
			partition_comment,
		from %s
		where 
		    table_id = %d
		order by 
		    partition_ordinal_position
		`,
		executor.Options{}.
			WithTxn(txnOp).
			WithDatabase(catalog.MO_CATALOG).
			WithAccountID(accountID),
	)
	if err != nil {
		return partition.PartitionMetadata{}, false, err
	}
	defer res.Close()

	var metadata partition.PartitionMetadata
	var found bool
	res.ReadRows(
		func(
			rows int,
			cols []*vector.Vector,
		) bool {
			found = true
			for i := 0; i < rows; i++ {
				method := executor.GetStringRows(cols[0])[i]
				metadata.Method = partition.PartitionMethod(partition.PartitionMethod_value[method])
				metadata.Description = executor.GetStringRows(cols[4])[i]

				metadata.Partitions = append(
					metadata.Partitions,
					partition.Partition{
						PartitionID: executor.GetFixedRows[uint64](cols[1])[i],
						Position:    executor.GetFixedRows[uint32](cols[2])[i],
						Expression:  executor.GetStringRows(cols[3])[i],
					})
			}
			return true
		},
	)
	return metadata, found, nil
}

func (s *storage) Create(
	ctx context.Context,
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			for _, p := range metadata.Partitions {
				err := s.createPartitionTable(
					def,
					metadata,
					p,
					txn,
				)
				if err != nil {
					return err
				}

				partitionID, err := s.getTableIDByTableNameAndDatabaseName(
					p.PartitionTableName,
					def.DbName,
					txn,
				)
				if err != nil {
					return err
				}
				p.PartitionID = partitionID

				err = s.createPartitionMetadata(
					def,
					metadata,
					p,
					txn,
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithAccountID(accountID),
	)
}

func (s *storage) Delete(
	ctx context.Context,
	metadata partition.PartitionMetadata,
	txnOp client.TxnOperator,
) error {
	db, _, _, err := s.eng.GetRelationById(
		ctx,
		txnOp,
		metadata.ID,
	)
	if err != nil {
		return err
	}

	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(db)
			for _, p := range metadata.Partitions {

			}

			return nil
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithAccountID(accountID),
	)
}

func (s *storage) createPartitionTable(
	def *plan.TableDef,
	metadata partition.PartitionMetadata,
	partition partition.Partition,
	txn executor.TxnExecutor,
) error {
	txn.Use(def.DbName)

	sql := getPartitionTableCreateSQL(
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
) string {
	return "TODO"
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
