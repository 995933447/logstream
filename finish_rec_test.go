package logstream

import (
	"fmt"
	"testing"
)

func TestFinishRec(t *testing.T) {
	rec, _ := newConsumeWaterMarkRec("D:\\log\\", "explv")
	fmt.Println(rec.getWaterMark())
}
