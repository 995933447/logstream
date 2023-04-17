package logstream

type Cfg struct {
	BaseDir              string   `validator:"required" json:"base_dir"`
	CompressTopics       []string `json:"compress_topics"`
	IdxFileMaxItemNum    uint32   `json:"idx_file_max_item_num"`
	DataFileMaxSize      string   `json:"data_file_max_size"` // format:XX/XXB/XXKB/XXK/XXM/XXMB/XXG/XXGB
	BlackTopics          []string `json:"black_topics"`
	WhiteTopics          []string `json:"white_topics"`
	MemMaxSize           string   `json:"mem_max_size"` // format:XX/XXB/XXKB/XXK/XXM/XXMB/XXG/XXGB
	MaxConcurrentForward uint32   `json:"max_concurrent_forward"`
}
