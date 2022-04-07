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
	"fmt"

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
	affectedRows := uint64(vector.Length(bat.Vecs[0]))
	fmt.Println("wangjian sql-1 is", affectedRows)

	// update calculate
	updateBatch := &batch.Batch{Attrs: p.UpdateAttrs}
	for _, etd := range p.UpdateList {
		fmt.Println("wangjian sql-1a is", etd)
		vec, _, err := etd.Eval(bat, proc)
		if err != nil {
			batch.Clean(updateBatch, proc.Mp)
			proc.Reg.InputBatch = &batch.Batch{}
			return false, err
		}
		updateBatch.Vecs = append(updateBatch.Vecs, vec)
		fmt.Println("wangjian sql-1b is", updateBatch.Vecs, updateBatch.Zs, updateBatch.Attrs)
	}
	for _, attr := range p.OtherAttrs {
		vec := batch.GetVector(bat, attr)
		vec.Ref++
		updateBatch.Vecs = append(updateBatch.Vecs, vec)
		fmt.Println("wangjian sql-1c is", updateBatch.Vecs, updateBatch.Zs, updateBatch.Attrs)
	}

	// delete tag
	for i, _ := range bat.Zs {
		bat.Zs[i] = -1
	}
	fmt.Println("wangjian sql0 is", bat.Zs, bat.Vecs, bat.Attrs)
	// update tag
	updateBatch.Zs = make([]int64, affectedRows)
	for i, _ := range updateBatch.Zs {
		updateBatch.Zs[i] = 1
	}
	fmt.Println("wangjian sql1 is", updateBatch.Zs, updateBatch.Vecs, updateBatch.Attrs)
	unionBat, err := bat.Append(proc.Mp, updateBatch)
	if err != nil {
		batch.Clean(unionBat, proc.Mp)
		batch.Clean(updateBatch, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	fmt.Println("wangjian sql2 is", unionBat.Zs, unionBat.Vecs, unionBat.Attrs)

	// write batch to the storage
	if err := p.Relation.Write(p.Ts, unionBat); err != nil {
		batch.Clean(unionBat, proc.Mp)
		batch.Clean(updateBatch, proc.Mp)
		proc.Reg.InputBatch = &batch.Batch{}
		return false, err
	}
	batch.Clean(unionBat, proc.Mp)
	batch.Clean(updateBatch, proc.Mp)
	proc.Reg.InputBatch = &batch.Batch{}

	p.M.Lock()
	p.AffectedRows += affectedRows
	p.M.Unlock()
	return false, nil
}


