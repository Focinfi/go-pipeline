package builders

import (
	"encoding/json"
	"testing"

	"github.com/Focinfi/pipeline/builders/calc"
	"github.com/Focinfi/pipeline/builders/etl"
)

func TestBuildHandler(t *testing.T) {
	tt := []struct {
		caseName string
		id       string
		name     string
		confJSON string
		want     interface{}
		hasErr   bool
	}{
		{
			caseName: "etl-json_extractor",
			id:       "foo",
			name:     BuilderETLJSONExtractor,
			confJSON: `{"confs": {"name": {"path": "data.name", "required": true, "default_value":"not_found"}}}`,
			want: etl.JSONExtractor{
				ID: "foo",
				Confs: map[string]etl.JSONExtractConf{
					"name": {
						Path:         "data.name",
						Required:     true,
						DefaultValue: "not_found",
					},
				},
			},
			hasErr: false,
		},
		{
			caseName: "calc-expr",
			id:       "foo",
			name:     BuilderCalcExpr,
			confJSON: `{"expr": "1+1"}`,
			want: calc.Expr{
				ID:   "foo",
				Expr: "1+1",
			},
			hasErr: false,
		},
		{
			caseName: "builder not found",
			id:       "foo",
			name:     "xx",
			confJSON: "{}",
			hasErr:   true,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			h, err := BuildHandler(item.id, item.name, item.confJSON)
			if item.hasErr {
				if err != nil {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantBytes, _ := json.MarshalIndent(item.want, "", "  ")
			gotBytes, _ := json.MarshalIndent(h, "", "  ")
			if string(gotBytes) != string(wantBytes) {
				t.Errorf("resp: want=%v, got=%v", string(wantBytes), string(gotBytes))
			}
		})
	}
}

func TestGetBuilderNames(t *testing.T) {
	got := GetBuilderNames()
	want := []string{BuilderCalcExpr, BuilderETLJSONExtractor}
	if len(got) != len(want) {
		t.Errorf("names: want=%v, got=%v", want, got)
	}
}
