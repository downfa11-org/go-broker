package disk

import (
	"errors"
	"fmt"
	"os"
)

func (dh *DiskHandler) GetFirstOffset() uint64 {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	if len(dh.segments) == 0 {
		return dh.CurrentSegment
	}
	return dh.segments[0]
}
func (dh *DiskHandler) GetLatestOffset() uint64 {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	return dh.AbsoluteOffset
}

// GetIndexFile returns the current index file
func (d *DiskHandler) GetIndexFile() *os.File {
	d.indexMu.Lock()
	defer d.indexMu.Unlock()

	return d.indexFile
}

func (d *DiskHandler) GetSegmentPath(baseOffset uint64) string {
	return fmt.Sprintf("%s_segment_%020d.log", d.BaseName, baseOffset)
}

func (d *DiskHandler) GetIndexPath(baseOffset uint64) string {
	return fmt.Sprintf("%s_segment_%020d.index", d.BaseName, baseOffset)
}

// OpenIndexFiles public method for testing
func (d *DiskHandler) OpenIndexFiles() error {
	return d.openIndexFiles()
}

// CloseIndexFiles public method for testing
func (d *DiskHandler) CloseIndexFiles() error {
	return d.closeIndexFiles()
}

// Close signals the flushLoop to terminate and cleans up resources.
func (d *DiskHandler) Close() error {
	d.closeOnce.Do(func() {
		var errs []error
		close(d.done)
		d.shutdown.Wait()
		errs = append(errs, d.drainErr)

		d.ioMu.Lock()
		defer d.ioMu.Unlock()
		if err := d.writeAvailabilityError(); err != nil {
			errs = append(errs, err)
		}

		if d.writer != nil {
			if err := d.writer.Flush(); err != nil {
				errs = append(errs, fmt.Errorf("data writer flush error: %w", err))
			}
		}

		if d.file != nil {
			if err := d.syncFile(d.file); err != nil {
				errs = append(errs, fmt.Errorf("data file sync error: %w", err))
			}
			if err := d.file.Close(); err != nil {
				errs = append(errs, fmt.Errorf("data file close error: %w", err))
			}
			d.file = nil
		}

		d.indexMu.Lock()
		if err := d.closeIndexFiles(); err != nil {
			errs = append(errs, fmt.Errorf("index cleanup error: %w", err))
		}
		d.indexMu.Unlock()

		if err := d.segmentReaders.close(); err != nil {
			errs = append(errs, fmt.Errorf("segment reader cache cleanup error: %w", err))
		}
		d.closeErr = errors.Join(errs...)
	})
	return d.closeErr
}
