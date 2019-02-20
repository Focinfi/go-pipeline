package pipeline

import (
	"encoding/json"
	"testing"

	"github.com/nsf/jsondiff"
)

func diff(a interface{}, b interface{}) (string, bool) {
	aJSON, err := json.Marshal(a)
	if err != nil {
		panic(err)
	}
	bJSON, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}

	opt := jsondiff.DefaultConsoleOptions()
	diff, text := jsondiff.Compare(aJSON, bJSON, &opt)
	return text, diff == jsondiff.FullMatch
}

func TestDiff(t *testing.T) {
	a := map[string]interface{}{
		"a": 1,
		"b": 2,
	}

	b := map[string]interface{}{
		"a": 2,
		"c": 0,
	}

	if text, ok := diff(a, a); !ok {
		t.Error("not ok: diff=\n", text)
	}

	if text, ok := diff(a, b); ok {
		t.Error("should not ok")
	} else {
		t.Log("diff:\n", text)
	}
}
