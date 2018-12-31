package etl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Focinfi/pipeline"
	"github.com/tidwall/gjson"
	"gopkg.in/go-playground/validator.v9"
)

var validatorIns = validator.New()

// JSONExtractConf configs one json path
// if data of one json path is not found, use the DefaultValue
type JSONExtractConf struct {
	Path         string      `json:"path" validate:"required"`
	Required     bool        `json:"required"`
	DefaultValue interface{} `json:"default_value"`
}

// JSONExtractor extract a json string into a map[string]interface{}
type JSONExtractor struct {
	ID    string                     `json:"id"`
	Confs map[string]JSONExtractConf `json:"confs" validate:"required,dive,required"`
}

// NewJSONExtractorByJSON creates and returns the pointer of JSONExtractor
func NewJSONExtractorByJSON(id string, confJSON string) (*JSONExtractor, error) {
	// unmarshal
	extractor := &JSONExtractor{}
	if err := json.Unmarshal([]byte(confJSON), extractor); err != nil {
		return nil, err
	}

	// validate
	if err := validatorIns.Struct(extractor); err != nil {
		return nil, err
	}

	extractor.ID = id
	return extractor, nil
}

// Handle assumes the args.InValue is a JSON string,
// parses it using gjson.Parse and build a map[string]interface{} for resp.OutValue,
// returns non-nil error when a Required data of one conf.Path is not found
func (je JSONExtractor) Handle(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
	f := gjson.Parse(fmt.Sprint(args.InValue))
	data := make(map[string]interface{}, len(je.Confs))

	for k, conf := range je.Confs {
		v := f.Get(conf.Path)
		if v.Value() != nil {
			data[k] = v.Value()
			continue
		}

		if conf.Required {
			return nil, errors.New("data lost, path=" + conf.Path)
		}

		data[k] = conf.DefaultValue
	}

	return &pipeline.Resp{
		OutValue: data,
		Params:   args.Params,
	}, nil
}
