package filebeat

import (
	"github.com/songjiao/logzoom/input"
)

func init() {
	input.Register("filebeat", New)
}
