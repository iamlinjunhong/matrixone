// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

type VisitPlanRule interface {
	MatchNode(*Node) bool
	IsApplyExpr() bool
	ApplyNode(*Node) error
	ApplyExpr(*Expr) (*Expr, error)
}

type VisitPlan struct {
	plan  *Plan
	rules []VisitPlanRule
}

func NewVisitPlan(pl *Plan, rules []VisitPlanRule) *VisitPlan {
	return &VisitPlan{
		plan:  pl,
		rules: rules,
	}
}

func (vq *VisitPlan) visitNode(qry *Query, node *Node) error {
	for i := range node.Children {
		if err := vq.visitNode(qry, qry.Nodes[node.Children[i]]); err != nil {
			return err
		}
	}

	for _, rule := range vq.rules {
		if rule.MatchNode(node) {
			err := rule.ApplyNode(node)
			if err != nil {
				return err
			}
		} else if rule.IsApplyExpr() {
			err := vq.exploreNode(rule, node)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (vq *VisitPlan) exploreNode(rule VisitPlanRule, node *Node) error {
	var err error
	if node.Limit != nil {
		node.Limit, err = rule.ApplyExpr(node.Limit)
		if err != nil {
			return err
		}
	}

	if node.Offset != nil {
		node.Offset, err = rule.ApplyExpr(node.Offset)
		if err != nil {
			return err
		}
	}

	for i := range node.OnList {
		node.OnList[i], err = rule.ApplyExpr(node.OnList[i])
		if err != nil {
			return err
		}
	}

	for i := range node.FilterList {
		node.FilterList[i], err = rule.ApplyExpr(node.FilterList[i])
		if err != nil {
			return err
		}
	}

	for i := range node.ProjectList {
		node.ProjectList[i], err = rule.ApplyExpr(node.ProjectList[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func (vq *VisitPlan) Visit() error {
	switch pl := vq.plan.Plan.(type) {
	case *Plan_Query:
		qry := pl.Query
		if len(qry.Steps) == 0 {
			return nil
		}

		for _, step := range qry.Steps {
			err := vq.visitNode(qry, qry.Nodes[step])
			if err != nil {
				return err
			}
		}

	case *plan.Plan_Ins:
		var err error
		for _, rule := range vq.rules {
			if rule.IsApplyExpr() {
				for _, column := range pl.Ins.Columns {
					for i := range column.Column {
						column.Column[i], err = rule.ApplyExpr(column.Column[i])
						if err != nil {
							return err
						}
					}
				}
			}
		}

	default:
		// do nothing

	}

	return nil
}
