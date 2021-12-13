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

package handler

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/protocol"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func recoverScope(ps protocol.Scope, proc *process.Process) *compile.Scope {
	s := new(compile.Scope)
	s.Instructions = ps.Ins
	s.Magic = ps.Magic
	if s.Magic == compile.Remote {
		s.Magic = compile.Merge
	}
	gm := guest.New(proc.Mp.Gm.Limit, proc.Mp.Gm.Mmu)
	s.Proc = process.New(mheap.New(gm))
	s.Proc.Lim = proc.Lim
	s.Proc.Reg.MergeReceivers = make([]*process.WaitRegister, len(ps.PreScopes))
	{
		for i, j := 0, len(ps.PreScopes); i < j; i++ {
			s.Proc.Reg.MergeReceivers[i] = &process.WaitRegister{
				Ch: make(chan *batch.Batch, 8),
			}
		}
	}
	s.PreScopes = make([]*compile.Scope, len(ps.PreScopes))
	for i := range ps.PreScopes {
		s.PreScopes[i] = recoverScope(ps.PreScopes[i], proc)
	}
	return s
}
