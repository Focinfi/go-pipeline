go-pipeline
------
[![Go Report Card](https://goreportcard.com/badge/github.com/Focinfi/go-pipeline)](https://goreportcard.com/report/github.com/Focinfi/go-pipeline)

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
