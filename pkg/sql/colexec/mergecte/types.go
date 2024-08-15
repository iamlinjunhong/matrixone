// Copyright 2021 Matrix Origin
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

package mergecte

import (
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var _ vm.Operator = new(MergeCTE)

const (
	sendInitial   = 0
	sendLastTag   = 1
	sendRecursive = 2
)

type container struct {
	buf        *batch.Batch
	bats       []*batch.Batch
	nodeCnt    int32
	curNodeCnt int32
	status     int32
	last       bool
	freeBats   []*batch.Batch
	i          int
}

type MergeCTE struct {
	ctr container

	vm.OperatorBase
}

func (mergeCTE *MergeCTE) GetOperatorBase() *vm.OperatorBase {
	return &mergeCTE.OperatorBase
}

func init() {
	reuse.CreatePool[MergeCTE](
		func() *MergeCTE {
			return &MergeCTE{}
		},
		func(a *MergeCTE) {
			*a = MergeCTE{}
		},
		reuse.DefaultOptions[MergeCTE]().
			WithEnableChecker(),
	)
}

func (mergeCTE MergeCTE) TypeName() string {
	return opName
}

func NewArgument() *MergeCTE {
	return reuse.Alloc[MergeCTE](nil)
}

func (mergeCTE *MergeCTE) Release() {
	if mergeCTE != nil {
		reuse.Free[MergeCTE](mergeCTE, nil)
	}
}

func (mergeCTE *MergeCTE) Reset(proc *process.Process, pipelineFailed bool, err error) {
	mergeCTE.ctr.nodeCnt = int32(len(proc.Reg.MergeReceivers)) - 1
	mergeCTE.ctr.curNodeCnt = mergeCTE.ctr.nodeCnt
	mergeCTE.ctr.status = sendInitial
	mergeCTE.ctr.i = 0
}

func (mergeCTE *MergeCTE) Free(proc *process.Process, pipelineFailed bool, err error) {
	for _, bat := range mergeCTE.ctr.freeBats {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}
}
