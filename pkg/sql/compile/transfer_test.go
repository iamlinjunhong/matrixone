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

package compile

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/sql/protocol"
	"matrixone/pkg/vm/engine"
	"testing"
)

func TestScope(t *testing.T) {
	var buf bytes.Buffer
	cs := &Scope{
		Magic: CreateDatabase,
		DataSource: &Source{
			IsMerge: true,
			SchemaName: "db1",
			RelationName: "table1",
			RefCounts: []uint64{1, 23423, 543234},
			Attributes: []string{"col1", "col2", "col3"},
		},
		PreScopes: nil,
		NodeInfo: engine.Node{
			Id: "node id",
			Addr: "engine address",
		},
		Instructions: nil,
	}
	transScope := Transfer(cs)
	err := protocol.EncodeScope(transScope, &buf)
	require.NoError(t, err)
	ResultScope, _, err := protocol.DecodeScope(buf.Bytes())
	require.NoError(t, err)
	// Magic
	if ResultScope.Magic != CreateDatabase{
		t.Errorf("Decode Scope type failed.")
		return
	}
	// DataSource
	if ResultScope.DataSource.IsMerge != true {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	if ResultScope.DataSource.SchemaName != "db1" {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	if ResultScope.DataSource.RelationName != "table1" {
		t.Errorf("Decode Scope DataSource failed.")
		return
	}
	for i, v := range ResultScope.DataSource.RefCounts {
		if v != cs.DataSource.RefCounts[i] {
			t.Errorf("Decode Scope DataSource failed.")
			return
		}
	}
	for i, v := range ResultScope.DataSource.Attributes {
		if v != cs.DataSource.Attributes[i] {
			t.Errorf("Decode Scope DataSource failed.")
			return
		}
	}
	// NodeInfo
	if ResultScope.NodeInfo.Id != "node id" {
		t.Errorf("Decode Scope NodeInfo failed.")
		return
	}
	if ResultScope.NodeInfo.Addr != "engine address" {
		t.Errorf("Decode Scope NodeInfo failed.")
		return
	}
}