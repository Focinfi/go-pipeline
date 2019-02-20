package pipeline

import (
	"testing"
	"time"
)

func TestHandleRes_Copy(t *testing.T) {
	res := &HandleRes{
		Status:  HandleStatusOK,
		Message: "OK",
		Meta: map[string]interface{}{
			"foo": "bar",
		},
		Data: time.Now().Unix(),
	}

	resCopy, err := res.Copy()
	if err != nil {
		t.Fatal(err)
	}

	result, ok := diff(res, resCopy)

	if !ok {
		t.Error("diff: \n", result)
	}
}
