//go:build windows
// +build windows

package disk

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
)

func (d *DiskHandler) openSegment() error {
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	f, err := os.OpenFile(fmt.Sprintf("%s_segment_%d.log", d.BaseName, d.CurrentSegment), flags, 0644)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriter(f)
	return nil
}

func (d *DiskHandler) SendCurrentSegmentToConn(conn net.Conn) error {
	d.mu.Lock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	defer d.mu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			return err
		}
	}
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			return err
		}
	}
	if _, err := d.file.Seek(0, 0); err != nil {
		return err
	}
	_, err := io.Copy(conn, d.file)
	return err
}
