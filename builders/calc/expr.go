package calc

import (
	"context"
	"encoding/json"

	"github.com/Focinfi/pipeline"
	"gopkg.in/go-playground/validator.v9"
	"qlang.io/cl/qlang"
	_ "qlang.io/lib/builtin" // 导入 builtin 包
)

var validatorIns = validator.New()

// Expr contains the qlang expression
type Expr struct {
	ID   string `json:"-"`
	Expr string `json:"expr" validate:"required"`
}

// NewExprByJSON creates and returns the pointer of Expr
func NewExprByJSON(id string, confJSON string) (*Expr, error) {
	// unmarshal
	expr := &Expr{}
	if err := json.Unmarshal([]byte(confJSON), expr); err != nil {
		return nil, err
	}

	// validate
	if err := validatorIns.Struct(expr); err != nil {
		return nil, err
	}

	expr.ID = id
	return expr, nil
}

// Handle runs qlang script and set the result to resp.OutValue
func (e *Expr) Handle(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
	ql := qlang.New()
	ql.SetVar("in_value", args.InValue)
	err = ql.SafeExec([]byte("x = "+e.Expr), "")
	if err != nil {
		return nil, err
	}

	return &pipeline.Resp{
		OutValue: ql.Var("x"),
		Params:   args.Params,
	}, nil
}
