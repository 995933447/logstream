package logstream

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestParseMemSizeStrToUint32(t *testing.T) {
	sizeUint32, err := parseMemSizeStrToBytes("10G")
	if err != nil {
		t.Fatal(err)
		return
	}
	t.Log(sizeUint32)
}

func TestGrowMap(t *testing.T) {
	m := map[string]string{
		"a": "hello",
		"b": "world",
		"c": "ok",
	}
	for k, v := range m {
		r := rand.Int()
		m[fmt.Sprintf("%d", r)] = fmt.Sprintf("%d", r)
		fmt.Println(k, v)
	}
	fmt.Println(len(m))
}
