package e2e_cluster

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type backupEntry struct {
	Digest     [sha256.Size]byte
	Size, Mode int64
	UID, GID   int
	Kind       byte
}

func inspectBackup(reader io.Reader, root string) (map[string]backupEntry, error) {
	entries := make(map[string]backupEntry)
	archive := tar.NewReader(reader)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		name := strings.TrimSuffix(header.Name, "/")
		if path.Clean(name) != name || (name != root && !strings.HasPrefix(name, root+"/")) || strings.Contains(name, "\\") {
			return nil, fmt.Errorf("archive path outside persistence root: %q", header.Name)
		}
		if _, exists := entries[name]; exists {
			return nil, fmt.Errorf("duplicate archive entry: %s", name)
		}
		if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeDir {
			return nil, fmt.Errorf("unsupported archive entry: %s", name)
		}
		hash := sha256.New()
		if _, err := io.Copy(hash, archive); err != nil {
			return nil, err
		}
		entry := backupEntry{Size: header.Size, Mode: header.Mode, UID: header.Uid, GID: header.Gid, Kind: header.Typeflag}
		copy(entry.Digest[:], hash.Sum(nil))
		entries[name] = entry
	}
	if len(entries) < 2 {
		return nil, fmt.Errorf("empty persistence backup")
	}
	return entries, nil
}

func TestBackupArchiveValidation(t *testing.T) {
	for _, name := range []string{"logs/data", "../logs/data", "/logs/data", "logs/../data", "logs\\data"} {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			writer := tar.NewWriter(&buf)
			require.NoError(t, writer.WriteHeader(&tar.Header{Name: "logs/", Typeflag: tar.TypeDir, Mode: 0750}))
			require.NoError(t, writer.WriteHeader(&tar.Header{Name: name, Typeflag: tar.TypeReg, Size: 4, Mode: 0600, Uid: 1000, Gid: 1000}))
			_, err := writer.Write([]byte("data"))
			require.NoError(t, err)
			require.NoError(t, writer.Close())
			entries, err := inspectBackup(&buf, "logs")
			if name == "logs/data" {
				require.NoError(t, err)
				require.Equal(t, sha256.Sum256([]byte("data")), entries[name].Digest)
				require.Equal(t, 1000, entries[name].UID)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBackupArchiveRejectsInvalidEntries(t *testing.T) {
	for _, kind := range []string{"empty", "duplicate", "symlink", "truncated"} {
		t.Run(kind, func(t *testing.T) {
			var buf bytes.Buffer
			writer := tar.NewWriter(&buf)
			require.NoError(t, writer.WriteHeader(&tar.Header{Name: "logs/", Typeflag: tar.TypeDir}))
			switch kind {
			case "duplicate":
				require.NoError(t, writer.WriteHeader(&tar.Header{Name: "logs/", Typeflag: tar.TypeDir}))
			case "symlink":
				require.NoError(t, writer.WriteHeader(&tar.Header{Name: "logs/link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}))
			case "truncated":
				require.NoError(t, writer.WriteHeader(&tar.Header{Name: "logs/data", Typeflag: tar.TypeReg, Size: 1024}))
			}
			if kind != "truncated" {
				require.NoError(t, writer.Close())
			}
			_, err := inspectBackup(&buf, "logs")
			require.Error(t, err)
		})
	}
}
