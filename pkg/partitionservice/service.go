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

	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

type service struct {
	engine engine.Engine
}

func (s *service) Create(
	ctx context.Context,
	tableID uint64,
	txnOp client.TxnOperator,
) error {
	_, _, _, err := s.engine.GetRelationById(
		ctx,
		txnOp,
		tableID,
	)
	if err != nil {
		return err
	}

	return nil
}
