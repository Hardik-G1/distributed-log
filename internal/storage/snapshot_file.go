package storage

import (
	"fmt"
	"os"
	"path/filepath"
)

func writeSnapshotFile(location string, data []byte) error {
	directory := filepath.Dir(location)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create snapshot directory %w", err)
	}

	temporary, err := os.CreateTemp(directory, ".snapshot-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, location); err != nil {
		return fmt.Errorf("finalise snapshot file %w", err)
	}
	return nil
}
