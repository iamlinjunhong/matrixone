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

package timewin

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
	"testing"
)

// add unit tests for cases
type timeWinTestCase struct {
	arg  *TimeWin
	proc *process.Process
}

var (
	tcs []timeWinTestCase
)

func init() {
	tcs = []timeWinTestCase{
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: true,
				WEnd:   true,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: true,
				WEnd:   false,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
		{
			proc: testutil.NewProcessWithMPool("", mpool.MustNewZero()),
			arg: &TimeWin{
				WStart: false,
				WEnd:   false,
				Types: []types.Type{
					types.T_int32.ToType(),
				},
				Aggs: []aggexec.AggFuncExecExpression{
					aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{newExpression(1)}, nil),
				},
				TsType:   plan.Type{Id: int32(types.T_datetime)},
				Ts:       newExpression(0),
				EndExpr:  newExpression(0),
				Interval: makeInterval(),
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range tcs {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range tcs {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestTimeWin(t *testing.T) {
	for _, tc := range tcs {
		resetChildren(tc.arg)
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg)
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, _ = tc.arg.Call(tc.proc)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func resetChildren(arg *TimeWin) {
	bat := colexec.MakeMockTimeWinBatchs()
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func newExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func makeInterval() types.Datetime {
	t, _ := calcDatetime(5, 2)
	return t
}
