// Copyright 2021 - 2022 Matrix Origin
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

package function2

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func Test_fixedTypeCastRule1(t *testing.T) {
	inputs := []struct {
		shouldCast bool
		in         [2]types.Type
		want       [2]types.Type
	}{
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int32.ToType()},
			want:       [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},

		{
			shouldCast: false,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
			want: [2]types.Type{
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
		},

		// special rule, null + null
		// we just cast it as int64 + int64
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_any.ToType(), types.T_any.ToType()},
			want:       [2]types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
		},
	}

	for i, in := range inputs {
		msg := fmt.Sprintf("i = %d", i)

		cast, t1, t2 := fixedTypeCastRule1(in.in[0], in.in[1])
		require.Equal(t, in.shouldCast, cast, msg)
		if in.shouldCast {
			require.Equal(t, in.want[0], t1, msg)
			require.Equal(t, in.want[1], t2, msg)
		}
	}
}

func Test_fixedTypeCastRule2(t *testing.T) {
	inputs := []struct {
		shouldCast bool
		in         [2]types.Type
		want       [2]types.Type
	}{
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int32.ToType()},
			want:       [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},

		{
			shouldCast: false,
			in:         [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				types.T_float64.ToType(),
			},
			want: [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},

		{
			shouldCast: true,
			in: [2]types.Type{
				{Oid: types.T_decimal64, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
			want: [2]types.Type{
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 6},
				{Oid: types.T_decimal128, Width: 38, Size: 16, Scale: 4},
			},
		},

		// special rule, null / null
		// we just cast it as float64 / float64
		{
			shouldCast: true,
			in:         [2]types.Type{types.T_int64.ToType(), types.T_int32.ToType()},
			want:       [2]types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
		},
	}

	for i, in := range inputs {
		msg := fmt.Sprintf("i = %d", i)

		cast, t1, t2 := fixedTypeCastRule2(in.in[0], in.in[1])
		require.Equal(t, in.shouldCast, cast, msg)
		if in.shouldCast {
			require.Equal(t, in.want[0], t1, msg)
			require.Equal(t, in.want[1], t2, msg)
		}
	}
}

func Test_GetFunctionByName(t *testing.T) {
	type fInput struct {
		name string
		args []types.Type

		// expected
		shouldErr bool

		requireFid int32
		requireOid int32

		shouldCast bool
		requireTyp []types.Type

		requireRet types.Type
	}

	cs := []fInput{
		{
			name: "+", args: []types.Type{types.T_int8.ToType(), types.T_int16.ToType()},
			shouldErr:  false,
			requireFid: PLUS, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_int16.ToType(), types.T_int16.ToType()},
			requireRet: types.T_int16.ToType(),
		},

		{
			name: "+", args: []types.Type{types.T_int64.ToType(), types.T_int64.ToType()},
			shouldErr:  false,
			requireFid: PLUS, requireOid: 0,
			shouldCast: false,
			requireRet: types.T_int64.ToType(),
		},

		{
			name: "/", args: []types.Type{types.T_int8.ToType(), types.T_int16.ToType()},
			shouldErr:  false,
			requireFid: DIV, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_float64.ToType(), types.T_float64.ToType()},
			requireRet: types.T_float64.ToType(),
		},

		{
			name: "internal_numeric_scale", args: []types.Type{types.T_char.ToType()},
			shouldErr:  false,
			requireFid: INTERNAL_NUMERIC_SCALE, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_varchar.ToType()},
			requireRet: types.T_int64.ToType(),
		},

		{
			name: "internal_numeric_scale", args: []types.Type{types.T_char.ToType(), types.T_int64.ToType()},
			shouldErr: true,
		},

		{
			name: "iff", args: []types.Type{types.T_bool.ToType(), types.T_any.ToType(), types.T_int64.ToType()},
			shouldErr:  false,
			requireFid: IFF, requireOid: 0,
			shouldCast: true, requireTyp: []types.Type{types.T_bool.ToType(), types.T_int64.ToType(), types.T_int64.ToType()},
			requireRet: types.T_int64.ToType(),
		},
	}

	proc := testutil.NewProcess()
	for i, c := range cs {
		msg := fmt.Sprintf("%dth case", i)

		get, err := GetFunctionByName(proc.Ctx, c.name, c.args)
		if c.shouldErr {
			require.True(t, err != nil, msg)
		} else {
			require.NoError(t, err, msg)
			require.Equal(t, c.requireFid, get.fid, msg)
			require.Equal(t, c.requireOid, get.overloadId, msg)
			require.Equal(t, c.shouldCast, get.needCast, msg)
			if c.shouldCast {
				require.Equal(t, len(c.requireTyp), len(get.targetTypes), msg)
				for j := range c.requireTyp {
					require.Equal(t, c.requireTyp[j], get.targetTypes[j], msg)
				}
			}
			require.Equal(t, c.requireRet, get.retType, msg)
		}
	}
}

func Test_ShowFunctionsShouldBeRefactored(t *testing.T) {
	alreadyRegistered := make(map[int32]bool)
	fromIDtoName := make(map[int32][]string)
	for name, id := range functionIdRegister {
		if names, ok := fromIDtoName[id]; ok {
			fromIDtoName[id] = append(names, name)
		} else {
			fromIDtoName[id] = []string{name}
		}

		alreadyRegistered[id] = true
	}

	sets := [][]FuncNew{
		supportedBuiltins, supportedOperators, supportedAggregateFunctions,
		tempListForUnaryFunctions1, tempListForBinaryFunctions2,
	}

	for _, set := range sets {
		for _, f := range set {
			if f.checkFn == nil || len(f.Overloads) == 0 {
				if !f.isAggregate() {
					continue
				}
			}
			delete(alreadyRegistered, int32(f.functionId))
		}
	}

	// show how many function we should implement.
	fmt.Printf("there are still %d functions need to implement\n", len(alreadyRegistered))
	count := 1
	for id := range alreadyRegistered {
		fmt.Printf("%d: %v\n", count, fromIDtoName[id])
		count++
	}
}
