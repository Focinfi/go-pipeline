package builders

import (
	"errors"

	"github.com/Focinfi/pipeline"
	"github.com/Focinfi/pipeline/builders/calc"
	"github.com/Focinfi/pipeline/builders/etl"
)

var ErrBuilderNotFound = errors.New("builder not found")

// all contains all handler factories
var all = map[string]pipeline.HandlerBuilder{
	BuilderCalcExpr: pipeline.HandlerBuildFunc(func(id string, confJSON string) (pipeline.Handler, error) {
		return calc.NewExprByJSON(id, confJSON)
	}),
	BuilderETLJSONExtractor: pipeline.HandlerBuildFunc(func(id string, confJSON string) (pipeline.Handler, error) {
		return etl.NewJSONExtractorByJSON(id, confJSON)
	}),
}

// BuildHandler finds the builder with the give name and use it to build a handler
func BuildHandler(id string, name string, confJSON string) (pipeline.Handler, error) {
	builder, ok := GetBuilderOK(name)
	if !ok {
		return nil, ErrBuilderNotFound
	}

	return builder.BuildHandlerByJSON(id, confJSON)
}

// GetBuilderNames returns a list of all builders
func GetBuilderNames() []string {
	names := make([]string, 0, len(all))
	for name := range all {
		names = append(names, name)
	}
	return names
}

// GetBuilderOK get the builder for the given name
func GetBuilderOK(name string) (pipeline.HandlerBuilder, bool) {
	b, ok := all[name]
	return b, ok
}

// SetBuilder updates a builder for the given name
func SetBuilder(name string, builder pipeline.HandlerBuilder) {
	all[name] = builder
}

// SetBuilderAll reset all builders with the given builders
func SetBuilderAll(builders map[string]pipeline.HandlerBuilder) {
	all = builders
}
