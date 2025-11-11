package disk

import (
	"encoding/binary"
	"log"
	"time"
)

func (d *DiskHandler) flushLoop() {
	batch := make([]string, 0, d.batchSize)
	ticker := time.NewTicker(d.linger)
	defer ticker.Stop()

	for {
		select {
		case msg := <-d.writeCh:
			batch = append(batch, msg)
			if len(batch) >= d.batchSize {
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				d.writeBatch(batch)
				batch = batch[:0]
			}
		case <-d.done:
			draining := true
			for draining {
				if len(batch) >= d.batchSize {
					d.writeBatch(batch)
					batch = batch[:0]
					continue
				}
				select {
				case msg, ok := <-d.writeCh:
					if !ok {
						draining = false
						continue
					}
					batch = append(batch, msg)
				default:
					draining = false
				}
			}
			if len(batch) > 0 {
				d.writeBatch(batch)
			}
			if d.file != nil {
				d.writer.Flush()
				if err := d.file.Sync(); err != nil {
					log.Printf("ERROR: Sync failed during shutdown: %v", err)
				}
				d.file.Close()
			}
			return
		}
	}
}

func (d *DiskHandler) writeBatch(batch []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			log.Printf("FATAL: failed to open segment: %v", err)
			return
		}
	}

	var lenBuf [4]byte

	for _, msg := range batch {
		data := []byte(msg)
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

		if _, err := d.writer.Write(lenBuf[:]); err != nil {
			log.Printf("ERROR: writeBatch failed writing. length: %v", err)
			break
		}
		if _, err := d.writer.Write(data); err != nil {
			log.Printf("ERROR: writeBatch failed writing. data: %v", err)
			break
		}

		d.CurrentOffset += 4 + len(data)

		if d.CurrentOffset >= d.SegmentSize {
			if err := d.rotateSegment(); err != nil {
				log.Printf("ERROR: rotateSegment failed to rotate segment: %v", err)
			}
		}
	}

	if err := d.writer.Flush(); err != nil {
		log.Printf("ERROR: flush failed after batch: %v", err)
		return
	}
	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: sync failed after batch: %v", err)
		}
	}
}

func (d *DiskHandler) WriteDirect(msg string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.ioMu.Lock()
	defer d.ioMu.Unlock()

	var lenBuf [4]byte

	if d.file == nil {
		if err := d.openSegment(); err != nil {
			log.Printf("FATAL: failed to open segment: %v", err)
			return
		}
	}

	data := []byte(msg)
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

	totalLen := 4 + len(data)

	if d.CurrentOffset+totalLen > d.SegmentSize {
		if err := d.rotateSegment(); err != nil {
			log.Printf("FATAL: rotateSegment failed to rotate segment: %v", err)
			return
		}
	}

	if _, err := d.writer.Write(lenBuf[:]); err != nil {
		log.Printf("ERROR: writeDirect failed writing. length: %v", err)
		return
	}
	if _, err := d.writer.Write(data); err != nil {
		log.Printf("ERROR: writeDirect failed writing. data: %v", err)
		return
	}

	d.CurrentOffset += totalLen
	d.writer.Flush()

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: Failed to sync disk file: %v\n", err)
		}
	}
}

func (d *DiskHandler) rotateSegment() error {
	if d.writer != nil {
		if err := d.writer.Flush(); err != nil {
			log.Printf("ERROR: flush failed during segment rotation: %v", err)
		}
	}
	if d.file != nil {
		if err := d.file.Close(); err != nil {
			log.Printf("ERROR: close failed during segment rotation: %v", err)
		}
	}
	d.CurrentSegment++
	d.CurrentOffset = 0
	return d.openSegment()
}

func (d *DiskHandler) Flush() {
	batch := make([]string, 0, len(d.writeCh))
	for {
		select {
		case msg := <-d.writeCh:
			batch = append(batch, msg)
		default:
			goto perform_write
		}
	}

perform_write:
	if len(batch) > 0 {
		d.writeBatch(batch)
		return
	}

	d.ioMu.Lock()
	defer d.ioMu.Unlock()
	if d.writer != nil {
		d.writer.Flush()
	}

	if d.file != nil {
		if err := d.file.Sync(); err != nil {
			log.Printf("ERROR: Failed to sync disk file: %v\n", err)
		}
	}
}
