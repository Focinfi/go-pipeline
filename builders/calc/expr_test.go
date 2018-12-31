package calc

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Focinfi/pipeline"
)

func TestNewExprByJSON(t *testing.T) {
	tt := []struct {
		caseName string
		id       string
		confJSON string
		wantExpr Expr
		hasErr   bool
	}{
		{
			caseName: "normal",
			id:       "1+1",
			confJSON: `{"expr": "1 + 1"}`,
			wantExpr: Expr{
				ID:   "1+1",
				Expr: `1 + 1`,
			},
			hasErr: false,
		},
		{
			caseName: "returns non-nil error when wrong json format",
			hasErr:   true,
		},
		{
			caseName: "returns non-nil error when expr is empty",
			id:       "foo",
			confJSON: `{}`,
			hasErr:   true,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			expr, err := NewExprByJSON(item.id, item.confJSON)
			if item.hasErr {
				if err != nil {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			if *expr != item.wantExpr {
				t.Errorf("expr: want=%v, got=%v", item.wantExpr, *expr)
			}
		})
	}
}

func TestExpr_Handle(t *testing.T) {
	tt := []struct {
		caseName string
		inValue  interface{}
		expr     string
		outValue interface{}
		hasErr   bool
	}{
		{
			caseName: "normal value",
			inValue:  5,
			expr:     "in_value > 1",
			outValue: true,
			hasErr:   false,
		},
		{
			caseName: "normal expr",
			inValue:  []interface{}{1, 2},
			expr:     "float64(in_value[0]) / float64(in_value[1])",
			outValue: 0.5,
			hasErr:   false,
		},
		{
			caseName: "expr result error",
			inValue:  0,
			expr:     "1 / in_value",
			hasErr:   true,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			h := &Expr{Expr: item.expr}
			resp, err := h.Handle(context.Background(), pipeline.Args{InValue: item.inValue})
			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantJSONBytes, _ := json.Marshal(item.outValue)
			gotJSONBytes, _ := json.Marshal(resp.OutValue)

			if string(gotJSONBytes) != string(wantJSONBytes) {
				t.Errorf("out value: want=%v, got=%v", string(wantJSONBytes), string(gotJSONBytes))
			}

		})
	}
}
