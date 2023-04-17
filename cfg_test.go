package logstream

import (
	"fmt"
	"testing"
)

func TestCfg(t *testing.T) {
	var cfg Cfg
	ChangeCfg(&cfg)
	fmt.Println(cfg)
}

func ChangeCfg(cfg *Cfg) {
	*cfg = Cfg{
		BaseDir: "hahahah",
	}
	fmt.Println(cfg)
}
