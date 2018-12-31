package line

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Focinfi/pipeline"
	"github.com/Focinfi/pipeline/builders"
	"github.com/Focinfi/pipeline/builders/etl"
	"github.com/Focinfi/pipeline/parallel"
)

func TestNewHandlers(t *testing.T) {
	tt := []struct {
		caseName   string
		id         string
		confJSON   string
		handlerMap map[string]pipeline.Handler
		wantResp   *Handlers
		hasErr     bool
	}{
		{
			caseName: "confJSON wrong format",
			confJSON: "x{}",
			hasErr:   true,
		},
		{
			caseName: "confJSON wrong format",
			confJSON: "{}",
			hasErr:   true,
		},
		{
			caseName: "handler not found",
			confJSON: `[{"ID": "unknown-handler"}]`,
			hasErr:   true,
		},
		{
			caseName: "pipe not found",
			confJSON: `[{"pipe_name": "unknown-pipe"}]`,
			hasErr:   true,
		},
		{
			caseName: "all pipes",
			id:       "test-line",
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
			wantResp: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:       "test-line_etl-json_extractor",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
						ID:       "test-line_etl-json_extractor",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
			},
		},
		{
			caseName: "all existing Handlers",
			id:       "test-line",
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
					ID: "test-line",
					Confs: map[string]etl.JSONExtractConf{
						"name": {
							Path:     "data.name",
							Required: true,
						},
					},
				},
				"h2": &etl.JSONExtractor{
					ID: "test-line",
					Confs: map[string]etl.JSONExtractConf{
						"age": {
							Path:     "data.age",
							Required: true,
						},
					},
				},
			},
			wantResp: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:       "h1",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
							ID: "test-line",
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
			},
			hasErr: false,
		},
		{
			caseName: "mix pipe and handler",
			id:       "test-line",
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
					ID: "test-line",
					Confs: map[string]etl.JSONExtractConf{
						"name": {
							Path:     "data.name",
							Required: true,
						},
					},
				},
			},
			wantResp: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:       "h1",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
						ID:       "test-line_etl-json_extractor",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
			},
			hasErr: false,
		},
		{
			caseName: "has parallel Handlers",
			id:       "test-line",
			confJSON: `[
						{
							"required": true,
							"ID": "h1",
							"time_out_millisecond": 1000,
							"default_value": {"name": "not_found"}
						},
						[ 
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
						]
                      ]`,
			handlerMap: map[string]pipeline.Handler{
				"h1": &etl.JSONExtractor{
					ID: "test-line",
					Confs: map[string]etl.JSONExtractConf{
						"name": {
							Path:     "data.name",
							Required: true,
						},
					},
				},
			},
			wantResp: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:       "h1",
						Required: true,
						Handler: &etl.JSONExtractor{
							ID: "test-line",
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
						ID: "test-line",
						Handler: parallel.Handlers{
							{
								ID:       "test-line_etl-json_extractor",
								Required: true,
								Handler: &etl.JSONExtractor{
									ID: "test-line",
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
					},
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

			wantRespStr, _ := json.MarshalIndent(&item.wantResp, "", "	")
			gotRespStr, _ := json.MarshalIndent(&handlers, "", "	")
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
		hasErr       bool
		handlers     *Handlers
		args         pipeline.Args
		resp         pipeline.Resp
	}{
		{
			caseName:     "all required handlers returns",
			procDuration: time.Millisecond * 120,
			hasErr:       false,
			handlers: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
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
						ID: "h2",
						Handler: parallel.Handlers{
							{
								ID:                 "h2_1",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
								}),
							},
							{
								ID:                 "h2_2",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 3}, nil
								}),
							},
						},
					},
				},
			},
			resp: pipeline.Resp{
				OutValue: []interface{}{2, 3},
			},
		},
		{
			caseName:     "one required handler failed",
			procDuration: time.Millisecond * 120,
			hasErr:       true,
			handlers: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:                 "h1",
						Required:           true,
						TimeOutMillisecond: 40,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							time.Sleep(time.Millisecond * 50)
							return &pipeline.Resp{OutValue: 1}, nil
						}),
					},
					{
						ID: "h2",
						Handler: parallel.Handlers{
							{
								ID:                 "h2_1",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
								}),
							},
							{
								ID:                 "h2_2",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 3}, nil
								}),
							},
						},
					},
				},
			},
			resp: pipeline.Resp{
				OutValue: []interface{}{2, 3},
			},
		},
		{
			caseName:     "one non-required handler failed",
			procDuration: time.Millisecond * 120,
			hasErr:       false,
			handlers: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:                 "h1",
						Required:           false,
						TimeOutMillisecond: 60,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							time.Sleep(time.Millisecond * 50)
							return nil, errors.New("failed")
						}),
					},
					{
						ID: "h2",
						Handler: parallel.Handlers{
							{
								ID:                 "h2_1",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
								}),
							},
							{
								ID:                 "h2_2",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 3}, nil
								}),
							},
						},
					},
				},
			},
			resp: pipeline.Resp{
				OutValue: []interface{}{4, 6},
			},
		},
		{
			caseName:     "one non-required handler timeout",
			procDuration: time.Millisecond * 120,
			hasErr:       false,
			handlers: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:                 "h1",
						Required:           false,
						TimeOutMillisecond: 40,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							time.Sleep(time.Millisecond * 50)
							return &pipeline.Resp{OutValue: 1}, nil
						}),
					},
					{
						ID: "h2",
						Handler: parallel.Handlers{
							{
								ID:                 "h2_1",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
								}),
							},
							{
								ID:                 "h2_2",
								Required:           true,
								TimeOutMillisecond: 100,
								Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
									time.Sleep(time.Millisecond * 50)
									return &pipeline.Resp{OutValue: args.InValue.(int) * 3}, nil
								}),
							},
						},
					},
				},
			},
			resp: pipeline.Resp{
				OutValue: []interface{}{4, 6},
			},
		},
		{
			caseName:     "last non-required handler failed",
			procDuration: time.Millisecond * 120,
			hasErr:       false,
			handlers: &Handlers{
				ID: "test-line",
				Handlers: []pipeline.Option{
					{
						ID:                 "h1",
						Required:           false,
						TimeOutMillisecond: 60,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							time.Sleep(time.Millisecond * 50)
							return nil, errors.New("failed")
						}),
					},
				},
			},
			resp: pipeline.Resp{
				OutValue: 2,
			},
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			now := time.Now()
			resp, err := item.handlers.Handle(context.Background(), item.args)
			procDuration := time.Now().Sub(now)

			if procDuration > item.procDuration {
				t.Errorf("proc duration: want<%v, got=%v", item.procDuration, procDuration)
			}

			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantRespB, _ := json.Marshal(item.resp)
			gotRespB, _ := json.Marshal(resp)
			if string(gotRespB) != string(wantRespB) {
				t.Errorf("resp: want=%#v, got=%#v", string(wantRespB), string(gotRespB))
			}
		})
	}
}

func TestHandlers_HandleVerbosely(t *testing.T) {
	tt := []struct {
		caseName string
		args     pipeline.Args
		handlers *Handlers
		resp     *pipeline.Resp
		logs     []*pipeline.RespLog
		hasErr   bool
	}{
		{
			caseName: "all required handlers ok",
			args: pipeline.Args{
				InValue: 1,
			},
			handlers: &Handlers{
				ID: "*2^2",
				Handlers: []pipeline.Option{
					{
						ID:                 "*2",
						Required:           true,
						TimeOutMillisecond: 60,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
						}),
					},
					{
						ID:                 "^2",
						Required:           true,
						TimeOutMillisecond: 60,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return &pipeline.Resp{OutValue: args.InValue.(int) * args.InValue.(int)}, nil
						}),
					},
				},
			},
			resp: &pipeline.Resp{
				OutValue: 4,
			},
			logs: []*pipeline.RespLog{
				{
					Out: `{"out_value":2}`,
				},
				{
					Out: `{"out_value":4}`,
				},
			},
			hasErr: false,
		},
		{
			caseName: "one required handler failed",
			args: pipeline.Args{
				InValue: 1,
			},
			handlers: &Handlers{
				ID: "*2^2",
				Handlers: []pipeline.Option{
					{
						ID:                 "*2",
						Required:           true,
						TimeOutMillisecond: 60,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return &pipeline.Resp{OutValue: args.InValue.(int) * 2}, nil
						}),
					},
					{
						ID:                 "^2",
						Required:           true,
						TimeOutMillisecond: 60,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return nil, errors.New("^2 failed")
						}),
					},
				},
			},
			logs: []*pipeline.RespLog{
				{
					Out: `{"out_value":2}`,
				},
				{
					Out: `null`,
					Err: errors.New("^2 failed"),
				},
			},
			hasErr: true,
		},
		{
			caseName: "non-required handler failed",
			args: pipeline.Args{
				InValue: 1,
			},
			handlers: &Handlers{
				ID: "*2^2*3",
				Handlers: []pipeline.Option{
					{
						ID:                 "*2",
						Required:           false,
						TimeOutMillisecond: 10,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							time.Sleep(time.Millisecond * 20)
							return &pipeline.Resp{OutValue: args.InValue.(int) * 2, Params: args.Params}, nil
						}),
					},
					{
						ID:                 "^2",
						Required:           true,
						TimeOutMillisecond: 60,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return &pipeline.Resp{OutValue: args.InValue.(int) * args.InValue.(int)}, nil
						}),
					},
					{
						ID:                 "*3",
						Required:           false,
						TimeOutMillisecond: 60,
						DefaultValue:       3,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							return nil, errors.New("*3 failed")
						}),
					},
				},
			},
			resp: &pipeline.Resp{
				OutValue: 3,
				Params: map[string]interface{}{
					"token": 101,
				},
			},
			logs: []*pipeline.RespLog{
				{
					Out: `{"out_value":2}`,
					Err: errors.New("timeout: handler_id=" + "*2"),
				},
				{
					Out: `{"out_value":4}`,
				},
				{
					Out: `{"out_value":3}`,
					Err: errors.New("*3 failed"),
				},
			},
			hasErr: false,
		},
		{
			caseName: "params changed",
			args: pipeline.Args{
				InValue: 1,
				Params: map[string]interface{}{
					"token": 100,
				},
			},
			handlers: &Handlers{
				ID: "*2^2",
				Handlers: []pipeline.Option{
					{
						ID:                 "*2",
						Required:           false,
						TimeOutMillisecond: 10,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							args.Params["token"] = 101
							return &pipeline.Resp{OutValue: args.InValue.(int) * 2, Params: args.Params}, nil
						}),
					},
					{
						ID:                 "^2",
						Required:           true,
						TimeOutMillisecond: 60,
						DefaultValue:       2,
						Handler: pipeline.HandleFunc(func(ctx context.Context, args pipeline.Args) (resp *pipeline.Resp, err error) {
							args.Params["token"] = 102
							return &pipeline.Resp{OutValue: args.InValue.(int) * args.InValue.(int), Params: args.Params}, nil
						}),
					},
				},
			},
			logs: []*pipeline.RespLog{
				{
					Out: `{"out_value":2,"params":{"token":101}}`,
				},
				{
					Out: `{"out_value":4,"params":{"token":102}}`,
				},
			},
			hasErr: false,
		},
	}

	for _, item := range tt {
		t.Run(item.caseName, func(t *testing.T) {
			_, logs, err := item.handlers.HandleVerbosely(context.Background(), item.args)

			if err != nil {
				if item.hasErr {
					t.Log(err)
				} else {
					t.Errorf("has err: want=%v, got=%v", item.hasErr, err)
				}
				return
			}

			wantRespB, _ := json.MarshalIndent(item.logs, "", "  ")
			gotRespB, _ := json.MarshalIndent(logs, "", "  ")
			if string(wantRespB) != string(gotRespB) {
				t.Errorf("resp: want=%v, got=%v", string(wantRespB), string(gotRespB))
			}

			wantLogsB, _ := json.MarshalIndent(item.logs, "", "  ")
			gotLogsB, _ := json.MarshalIndent(logs, "", "  ")
			if string(gotLogsB) != string(wantLogsB) {
				t.Errorf("logs: want=%v, got=%v", string(wantLogsB), string(gotLogsB))
			}
		})
	}
}
