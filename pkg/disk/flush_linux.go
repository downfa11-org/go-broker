//go:build linux
// +build linux

package disk

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"golang.org/x/sys/unix"
)

func (d *DiskHandler) openSegment() error {
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	f, err := os.OpenFile(fmt.Sprintf("%s_segment_%d.log", d.BaseName, d.CurrentSegment), flags, 0644)
	if err != nil {
		return err
	}
	d.file = f
	d.writer = bufio.NewWriter(f)

	// Linux: sequential access hint
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_SEQUENTIAL)
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

	info, err := d.file.Stat()
	if err != nil {
		return err
	}

	var offset int64 = 0
	size := info.Size()
	sysConn, ok := conn.(*net.TCPConn)
	if !ok {
		if _, err := d.file.Seek(0, 0); err != nil {
			log.Printf("ERROR: Seek failed: %v", err)
			return err
		}
		_, err := io.Copy(conn, d.file)
		return err
	}

	rawConn, err := sysConn.SyscallConn()
	if err != nil {
		return err
	}

	var sendErr error
	if err := rawConn.Control(func(fd uintptr) {
		inFd := int(d.file.Fd())
		outFd := int(fd)
		for offset < size {
			n, err := unix.Sendfile(outFd, inFd, &offset, int(size-offset))
			if err != nil {
				sendErr = err
				return
			}
			if n == 0 {
				break
			}
		}
	}); err != nil {
		log.Printf("ERROR: rawConn.Control failed: %v", err)
		return fmt.Errorf("rawConn.Control: %w", err)
	}
	return sendErr
}
