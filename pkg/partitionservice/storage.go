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

func NewStorage(
	exec executor.SQLExecutor,
	eng engine.Engine,
) PartitionStorage {
	return &storage{
		exec: exec,
		eng:  eng,
	}
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

	var metadata partition.PartitionMetadata
	var found bool
	err = s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			res, err := txn.Exec(
				fmt.Sprintf(
					`
						select 		          
							table_name                 
							partition_method           
							partition_expression       
							partition_description      
							partition_count         
						from %s
					 	where 
							table_id = %d
					`,
					catalog.MOPartitionMetadata,
					tableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}

			n := uint32(0)
			res.ReadRows(
				func(
					rows int,
					cols []*vector.Vector,
				) bool {
					found = true
					for i := 0; i < rows; i++ {
						metadata.TableID = tableID
						metadata.TableName = executor.GetStringRows(cols[0])[i]
						metadata.Method = partition.PartitionMethod(
							partition.PartitionMethod_value[executor.GetStringRows(cols[1])[i]],
						)
						metadata.Expression = executor.GetStringRows(cols[2])[i]
						metadata.Description = executor.GetStringRows(cols[3])[i]
						n = executor.GetFixedRows[uint32](cols[4])[i]
					}
					return true
				},
			)
			res.Close()

			if !found {
				return nil
			}

			res, err = txn.Exec(
				`
					select 
						partition_id              ,
						partition_table_name      ,
						partition_name            ,
						partition_ordinal_position,
						partition_comment         
					from %s
					where 
						primary_table_id = %d
					order by 
						partition_ordinal_position
				`,
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}

			res.ReadRows(
				func(
					rows int,
					cols []*vector.Vector,
				) bool {
					found = true
					for i := 0; i < rows; i++ {
						metadata.Partitions = append(
							metadata.Partitions,
							partition.Partition{
								PartitionID:        executor.GetFixedRows[uint64](cols[0])[i],
								PartitionTableName: executor.GetStringRows(cols[1])[i],
								Name:               executor.GetStringRows(cols[2])[i],
								Position:           executor.GetFixedRows[uint32](cols[3])[i],
								Comment:            executor.GetStringRows(cols[4])[i],
							},
						)
					}
					return true
				},
			)
			res.Close()

			if n != uint32(len(metadata.Partitions)) {
				panic(
					fmt.Sprintf("partition count not match, expect %d, got %d",
						n,
						len(metadata.Partitions)),
				)
			}

			return nil
		},
		executor.Options{}.
			WithTxn(txnOp).
			WithDatabase(catalog.MO_CATALOG).
			WithAccountID(accountID),
	)
	return metadata, found, err
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
			err = s.createPartitionMetadata(
				metadata,
				txn,
			)
			if err != nil {
				return err
			}

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
	accountID, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}

	return s.exec.ExecTxn(
		ctx,
		func(txn executor.TxnExecutor) error {
			txn.Use(catalog.MO_CATALOG)
			res, err := txn.Exec(
				fmt.Sprintf(
					"delete from %s where table_id = %d",
					catalog.MOPartitionMetadata,
					metadata.TableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()

			res, err = txn.Exec(
				fmt.Sprintf(
					"delete from %s where primary_table_id = %d",
					catalog.MOPartitionTables,
					metadata.TableID,
				),
				executor.StatementOption{},
			)
			if err != nil {
				return err
			}
			res.Close()

			txn.Use(metadata.DatabaseName)
			for _, p := range metadata.Partitions {
				res, err = txn.Exec(
					fmt.Sprintf(
						"drop table %s",
						p.PartitionTableName,
					),
					executor.StatementOption{},
				)
				if err != nil {
					return err
				}
				res.Close()
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
	// create partition table using primary table's schema
	createPartitionTable := func() error {
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

		partitionID, err := s.getTableIDByTableNameAndDatabaseName(
			partition.PartitionTableName,
			def.DbName,
			txn,
		)
		if err != nil {
			return err
		}
		partition.PartitionID = partitionID
		return nil
	}

	// add partition metadata to mo_catalog.mo_partitions
	addPartitionMetadata := func() error {
		txn.Use(catalog.MO_CATALOG)
		res, err := txn.Exec(
			fmt.Sprintf(
				`insert into %s.%s 
				(
					partition_id, 
					partition_table_name, 
					primary_table_id, 
					partition_name, 
					partition_ordinal_position, 
					partition_comment
				)
				values
				(
					%d,
					'%s', 
					%d, 
					'%s', 
					%d, 
					'%s',
				)`,
				catalog.MO_CATALOG,
				catalog.MOPartitionTables,
				partition.PartitionID,
				partition.PartitionTableName,
				metadata.TableID,
				partition.Name,
				partition.Position,
				partition.Comment,
			),
			executor.StatementOption{},
		)
		if err != nil {
			return err
		}

		res.Close()
		return nil
	}

	if err := createPartitionTable(); err != nil {
		return err
	}
	return addPartitionMetadata()
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
	metadata partition.PartitionMetadata,
	txn executor.TxnExecutor,
) error {
	txn.Use(catalog.MO_CATALOG)

	sql := getInsertMetadataSQL(
		metadata,
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
	metadata partition.PartitionMetadata,
) string {
	return fmt.Sprintf(`
		insert into %s.%s 
			(
				table_id,
				table_name,
				database_name,
				partition_method,
				partition_expression,
				partition_description,
				partition_count
			)
		values
			(
				%d, 
				'%s', 
				'%s',
				'%s', 
				'%s', 
				'%s',
				 %d
			)`,
		catalog.MO_CATALOG,
		catalog.MOPartitionMetadata,
		metadata.TableID,
		metadata.TableName,
		metadata.DatabaseName,
		metadata.Method.String(),
		metadata.Expression,
		metadata.Description,
		len(metadata.Partitions),
	)
}
