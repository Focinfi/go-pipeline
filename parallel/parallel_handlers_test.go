package parallel

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Focinfi/pipeline"
	"github.com/Focinfi/pipeline/builders"
	"github.com/Focinfi/pipeline/builders/etl"
)

func TestNewParallelHandlers(t *testing.T) {
	tt := []struct {
		caseName   string
		id         string
		confJSON   string
		handlerMap map[string]pipeline.Handler
		wantResp   Handlers
		hasErr     bool
	}{
		{
			caseName: "confJSON wrong format",
			confJSON: "x{}",
			hasErr:   true,
		},
		{
			caseName: "handler not found",
			confJSON: `[{"id": "unknown-handler"}]`,
			hasErr:   true,
		},
		{
			caseName: "pipe not found",
			confJSON: `[{"pipe_name": "unknown-pipe"}]`,
			hasErr:   true,
		},
		{
			caseName: "all required pipe handlers",
			id:       "test-parallel",
			confJSON: `[
						{
							"required": true,
							"pipe_name": "etl-json_extractor", 
							"pipe_conf": {
								"confs": {
									"name": {
										"path": "data.name",
										"required": true
									}
								}
							},
							"time_out_millisecond": 1000,
							"default_value": {"name": "not_found"}
						},
						{
							"required": true,
							"pipe_name": "etl-json_extractor", 
							"pipe_conf": {
								"confs": {
									"age": {
										"path": "data.age",
										"required": true
									}
								}
							},
							"time_out_millisecond": 1000,
							"default_value": {"age": 20}
						}
                       ]`,
			wantResp: Handlers{
				{
					ID:       "test-parallel_etl-json_extractor",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"name": {
								Path:     "data.name",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"name": "not_found"},
					PipeName:           builders.BuilderETLJSONExtractor,
					PipeConf:           []byte(`{"confs":{"name":{"path":"data.name","required": true}}}`),
				},
				{
					ID:       "test-parallel_etl-json_extractor",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"age": {
								Path:     "data.age",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"age": 20},
					PipeName:           builders.BuilderETLJSONExtractor,
					PipeConf:           []byte(`{"confs":{"age":{"path":"data.age","required": true}}}`),
				},
			},
			hasErr: false,
		},
		{
			caseName: "all existing handlers",
			id:       "test-parallel",
			confJSON: `[
						{
							"required": true,
							"id": "h1",
							"time_out_millisecond": 1000,
							"default_value": {"name": "not_found"}
						},
						{
							"required": true,
							"id": "h2",
							"time_out_millisecond": 1000,
							"default_value": {"age": 20}
						}
                      ]`,
			handlerMap: map[string]pipeline.Handler{
				"h1": &etl.JSONExtractor{
					ID: "test-parallel",
					Confs: map[string]etl.JSONExtractConf{
						"name": {
							Path:     "data.name",
							Required: true,
						},
					},
				},
				"h2": &etl.JSONExtractor{
					ID: "test-parallel",
					Confs: map[string]etl.JSONExtractConf{
						"age": {
							Path:     "data.age",
							Required: true,
						},
					},
				},
			},
			wantResp: Handlers{
				{
					ID:       "h1",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"name": {
								Path:     "data.name",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"name": "not_found"},
				},
				{
					ID:       "h2",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"age": {
								Path:     "data.age",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"age": 20},
				},
			},
			hasErr: false,
		},
		{
			caseName: "mix pipe and handler",
			id:       "test-parallel",
			confJSON: `[
						{
							"required": true,
							"id": "h1",
							"time_out_millisecond": 1000,
							"default_value": {"name": "not_found"}
						},
						{
							"required": true,
							"pipe_name": "etl-json_extractor", 
							"pipe_conf": {
								"confs": {
									"age": {
										"path": "data.age",
										"required": true
									}
								}
							},
							"time_out_millisecond": 1000,
							"default_value": {"age": 20}
						}
                      ]`,
			handlerMap: map[string]pipeline.Handler{
				"h1": &etl.JSONExtractor{
					ID: "test-parallel",
					Confs: map[string]etl.JSONExtractConf{
						"name": {
							Path:     "data.name",
							Required: true,
						},
					},
				},
			},
			wantResp: Handlers{
				{
					ID:       "h1",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"name": {
								Path:     "data.name",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"name": "not_found"},
				},
				{
					ID:       "test-parallel_etl-json_extractor",
					Required: true,
					Handler: &etl.JSONExtractor{
						ID: "test-parallel",
						Confs: map[string]etl.JSONExtractConf{
							"age": {
								Path:     "data.age",
								Required: true,
							},
						},
					},
					TimeOutMillisecond: 1000,
					DefaultValue:       map[string]interface{}{"age": 20},
					PipeName:           builders.BuilderETLJSONExtractor,
					PipeConf:           []byte(`{"confs":{"age":{"path":"data.age","required": true}}}`),
				},
			},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			handlers, err := NewHandlers(item.id, item.confJSON, item.handlerMap)
			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantRespStr, err := json.MarshalIndent(&item.wantResp, "", "	")
			if err != nil {
				t.Fatal(err)
			}
			gotRespStr, err := json.MarshalIndent(&handlers, "", "	")
			if err != nil {
				t.Fatal(err)
			}

			if string(gotRespStr) != string(wantRespStr) {
				t.Errorf("resp: want=%v, got=%v", string(wantRespStr), string(gotRespStr))
			}
		})
	}
}

func TestHandlers_Handle(t *testing.T) {
	tt := []struct {
		caseName     string
		procDuration time.Duration
		handlers     Handlers
		args         pipeline.Args
		resp         pipeline.Resp
		hasErr       bool
	}{
		{
			caseName:     "normal all required and returns val",
			procDuration: time.Millisecond * 50,
			handlers: Handlers{
				{
					ID:                 "h1",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: 1}, nil
					}),
				},
				{
					ID:                 "h2",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: "2"}, nil
					}),
				},
			},
			args:   pipeline.Args{},
			resp:   pipeline.Resp{OutValue: []interface{}{1, "2"}},
			hasErr: false,
		},
		{
			caseName:     "one required handler failed",
			procDuration: time.Millisecond * 50,
			handlers: Handlers{
				{
					ID:                 "h1",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 30)
						return nil, errors.New("error from h1")
					}),
				},
				{
					ID:                 "h2",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: "2"}, nil
					}),
				},
			},
			args:   pipeline.Args{},
			hasErr: true,
		},
		{
			caseName:     "one require handler timeout",
			procDuration: time.Millisecond * 50,
			handlers: Handlers{
				{
					ID:                 "h1",
					Required:           true,
					TimeOutMillisecond: 50,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 55)
						return &pipeline.Resp{OutValue: 1}, nil
					}),
				},
				{
					ID:                 "h2",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: "2"}, nil
					}),
				},
			},
			args:   pipeline.Args{},
			hasErr: true,
		},
		{
			caseName:     "one non-required handler failed",
			procDuration: time.Millisecond * 50,
			handlers: Handlers{
				{
					ID:                 "h1",
					Required:           false,
					DefaultValue:       0,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 30)
						return nil, errors.New("error from h1")
					}),
				},
				{
					ID:                 "h2",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: "2"}, nil
					}),
				},
			},
			args:   pipeline.Args{},
			resp:   pipeline.Resp{OutValue: []interface{}{0, "2"}},
			hasErr: false,
		},
		{
			caseName:     "one non-required handler timeout",
			procDuration: time.Millisecond * 50,
			handlers: Handlers{
				{
					ID:                 "h1",
					Required:           false,
					DefaultValue:       0,
					TimeOutMillisecond: 20,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 30)
						return &pipeline.Resp{OutValue: 1}, nil
					}),
				},
				{
					ID:                 "h2",
					Required:           true,
					TimeOutMillisecond: 100,
					Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
						time.Sleep(time.Millisecond * 50)
						return &pipeline.Resp{OutValue: "2"}, nil
					}),
				},
			},
			args:   pipeline.Args{},
			resp:   pipeline.Resp{OutValue: []interface{}{0, "2"}},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			now := time.Now()
			resp, err := item.handlers.Handle(context.Background(), item.args)
			procDuration := time.Now().Sub(now)

			if procDuration > item.procDuration*2 {
				t.Errorf("proc duration: want<%v, got=%v", item.procDuration*2, procDuration)
			}

			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantRespB, _ := json.MarshalIndent(item.resp, "", "	")
			gotRespB, _ := json.MarshalIndent(resp, "", "	")
			if string(gotRespB) != string(wantRespB) {
				t.Errorf("resp: want=%#v, got=%#v", string(wantRespB), string(gotRespB))
			}

		})
	}
}
