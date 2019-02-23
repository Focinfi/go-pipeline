go-pipeline
------
[![build status](https://travis-ci.com/Focinfi/go-pipeline.svg?branch=master)](https://circleci.com/Focinfi/go-pipeline)
[![Go Report Card](https://goreportcard.com/badge/github.com/Focinfi/go-pipeline)](https://goreportcard.com/report/github.com/Focinfi/go-pipeline)
[![codecov](https://codecov.io/gh/Focinfi/go-pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/Focinfi/go-pipeline)

Configurable data processing in golang. 


### Install
```bash
go get github.com/Focinfi/go-pipeline
```

### Processing Flow
![processing_flow](.github/pipeline.svg)

1. Handler-*: References a existing Handler
2. Builder-*: Builds a Handler with config
3. Independent Handlers can process parallelly

### Handler
```go
type HandleRes struct {
	Status  HandleStatus           `json:"status"`
	Message string                 `json:"message"`
	Meta    map[string]interface{} `json:"meta"`
	Data    interface{}            `json:"data"`
}

type Handler interface {
	Handle(ctx context.Context, reqRes *HandleRes) (respRes *HandleRes, err error)
}
```

### Pipe / Parallel / Line
1. `Pipe` 
    1. It is a `Handler`
    1. Instanced by config, contains a internal handler
    2. The internal handler can be built by a builder or refrenced by anther `Handler`
    3. Run the internal hanlder with timeout

2. `Parallel`
    1. It is a `Handler`
    1. Contains a list pipes of `Pipe`
    1. Parallelly run the every `Pipe.Handle`

3. `Line`
    1. It is a `Handler`
    1. Contains a list of `Pipe`
    1. Sequently run the every `Pipe.Handle`
    1. Create a Line with JSON