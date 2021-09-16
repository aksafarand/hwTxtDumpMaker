package configs

import (
	"bytes"
	"io"
)

type Config struct {
	FtpName      string `json:"ftpname"`
	RemoteServer string `json:"servername"`
	RemoteFolder string `json:"remotefolder"`
	RemoteUser   string `json:"remoteuser"`
	RemotePass   string `json:"remotepass"`
	FilePrefix   string `json:"fileprefix"`
	Region       string `json:"region"`
	DateFind     string
	Part         string `json:"part"`
}

func (c *Config) FillDate(cd string) {
	currentDate := cd + "0"
	if c.DateFind == "" {
		c.DateFind = currentDate
	}
}

type Table struct {
	Name      string
	Fpath     string
	Header    []string
	HeaderMap map[string]int64
	File      io.WriteCloser
	Buffer    *bytes.Buffer
}
