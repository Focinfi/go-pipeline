package etl

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Focinfi/pipeline"
)

func TestNewJSONExtractorByJSON(t *testing.T) {
	tt := []struct {
		caseName      string
		id            string
		confJSON      string
		wantExtractor JSONExtractor
		hasErr        bool
	}{
		{
			caseName: "normal",
			id:       "1+1",
			confJSON: `{"confs": {"name": {"path": "data.name", "required": true, "default_value": "not_found"}}}`,
			wantExtractor: JSONExtractor{
				ID: "1+1",
				Confs: map[string]JSONExtractConf{
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
			caseName: "returns non-nil error when wrong json format",
			hasErr:   true,
		},
		{
			caseName: "returns non-nil error when confs is empty",
			id:       "foo",
			confJSON: `{}`,
			hasErr:   true,
		},
		{
			caseName: "returns non-nil error when path is empty",
			id:       "foo",
			confJSON: `{"confs": {"name": {"data_path": "data.name", "required": true, "default_value": "not_found"}}}`,
			hasErr:   true,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			extractor, err := NewJSONExtractorByJSON(item.id, item.confJSON)
			if item.hasErr {
				if err != nil {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantRespBytes, _ := json.MarshalIndent(item.wantExtractor, "", "  ")
			gotRespBytes, _ := json.MarshalIndent(extractor, "", "  ")
			if string(wantRespBytes) != string(gotRespBytes) {
				t.Errorf("extractor: want=%v, got=%v", string(wantRespBytes), string(gotRespBytes))
			}
		})
	}
}

func TestJSONExtractor_Handle(t *testing.T) {
	tt := []struct {
		caseName      string
		jsonExtractor *JSONExtractor
		inValue       string
		outValue      map[string]interface{}
		hasErr        bool
	}{
		{
			caseName: "all required data are existing",
			jsonExtractor: &JSONExtractor{
				Confs: map[string]JSONExtractConf{
					"name": {
						Path:     "data.name",
						Required: true,
					},
					"tags": {
						Path:     "data.stats.tags",
						Required: true,
					},
				},
			},
			inValue: `{"data": {"name": "foo", "stats": {"tags": [1, 2]}}}`,
			outValue: map[string]interface{}{
				"name": "foo",
				"tags": []int{1, 2},
			},
			hasErr: false,
		},
		{
			caseName: "one required data missing",
			jsonExtractor: &JSONExtractor{
				Confs: map[string]JSONExtractConf{
					"name": {
						Path:     "data.name",
						Required: true,
					},
					"tags": {
						Path:     "data.stats.tags",
						Required: true,
					},
				},
			},
			inValue: `{"data": {"xname": "foo", "stats": {"tags": [1, 2]}}}`,
			hasErr:  true,
		},
		{
			caseName: "one non-required data missing",
			jsonExtractor: &JSONExtractor{
				Confs: map[string]JSONExtractConf{
					"name": {
						Path:     "data.name",
						Required: true,
					},
					"tags": {
						Path:     "data.stats.tags",
						Required: true,
					},
					"unknown": {
						Path:         "unknown",
						Required:     false,
						DefaultValue: 1,
					},
				},
			},
			inValue: `{"data": {"name": "foo", "stats": {"tags": [1, 2]}}}`,
			outValue: map[string]interface{}{
				"name":    "foo",
				"tags":    []int{1, 2},
				"unknown": 1,
			},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			resp, err := item.jsonExtractor.Handle(context.Background(), pipeline.Args{InValue: item.inValue})
			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantJSONBytes, _ := json.MarshalIndent(item.outValue, "", "  ")
			gotJSONBytes, _ := json.MarshalIndent(resp.OutValue, "", "  ")
			if string(gotJSONBytes) != string(wantJSONBytes) {
				t.Errorf("out value: want=%v, got=%v", string(wantJSONBytes), string(gotJSONBytes))
			}
		})
	}
}
