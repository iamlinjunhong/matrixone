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

package updateTag

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func String(arg interface{}, buf *bytes.Buffer) {
	buf.WriteString("update table rows")
}

func Prepare(_ *process.Process, _ interface{}) error {
	return nil
}

func Call(proc *process.Process, arg interface{}) (bool, error) {
	p := arg.(*Argument)
	bat := proc.Reg.InputBatch
	if bat == nil || len(bat.Zs) == 0 {
		return false, nil
	}

	if p.HasModifyPriKey {
		bat.Zs = []int64{-1, -1}
		if err := p.Relation.Write(p.Ts, bat); err != nil {
			return false, err
		}
	}
	updateBatch := &batch.Batch{Attrs: p.UpdateAttrs, Zs: []int64{-1, 1}}
	for _, etd := range p.UpdateList {
		vec, _, err := etd.Eval(bat, proc)
		if err != nil {
			return false, err
		}
		updateBatch.Vecs = append(updateBatch.Vecs, vec)
	}
	if err := p.Relation.Write(p.Ts, updateBatch); err != nil {
		return false, err
	}

	affectedRows := uint64(vector.Length(bat.Vecs[0]))
	p.M.Lock()
	p.AffectedRows += affectedRows
	p.M.Unlock()
	return false, nil
}


