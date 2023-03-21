package tuf

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

var getRemoteRoot = func() string { return DefaultRemoteRoot }

// Target Implementations
type targetImpl interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
}

func newFileImpl() targetImpl {
	memTargets := &memoryCache{}
	if noCache() {
		return memTargets
	}
	// Otherwise use a disk-cache with in-memory cached targets.
	targetsDir := cachedTargetsDir(rootCacheDir())
	metadataDir := cachedMetadataDir(rootCacheDir())
	// make sure the folders used to keep the metadata and targets exist
	if err := os.MkdirAll(targetsDir, 0o700); err != nil {
		return nil
	}
	if err := os.MkdirAll(metadataDir, 0o700); err != nil {
		return nil
	}
	return &diskCache{
		base:   targetsDir,
		memory: memTargets,
	}
}

// In-memory cache for targets
type memoryCache struct {
	targets map[string][]byte
}

func (m *memoryCache) Set(p string, b []byte) error {
	if m.targets == nil {
		m.targets = map[string][]byte{}
	}
	m.targets[p] = b
	return nil
}

func (m *memoryCache) Get(p string) ([]byte, error) {
	if m.targets == nil {
		return nil, fmt.Errorf("no cached targets available, cannot retrieve %s", p)
	}
	b, ok := m.targets[p]
	if !ok {
		return nil, fmt.Errorf("missing cached target %s", p)
	}
	return b, nil
}

// On-disk cache for targets
type diskCache struct {
	// Base directory for accessing targets.
	base string
	// An in-memory map of targets that are kept in sync.
	memory *memoryCache
}

func (d *diskCache) Get(p string) ([]byte, error) {
	// Read from the in-memory cache first.
	if b, err := d.memory.Get(p); err == nil {
		return b, nil
	}
	fp := filepath.FromSlash(filepath.Join(d.base, p))
	return os.ReadFile(fp)
}

func (d *diskCache) Set(p string, b []byte) error {
	if err := d.memory.Set(p, b); err != nil {
		return err
	}

	fp := filepath.FromSlash(filepath.Join(d.base, p))
	if err := os.MkdirAll(filepath.Dir(fp), 0o700); err != nil {
		return fmt.Errorf("creating targets dir: %w", err)
	}

	return os.WriteFile(fp, b, 0o600)
}

func cachedTargetsDir(cacheRoot string) string {
	return filepath.FromSlash(filepath.Join(cacheRoot, "targets"))
}

func cachedMetadataDir(cacheRoot string) string {
	// return filepath.FromSlash(filepath.Join(cacheRoot, "metadata"))
	return filepath.FromSlash(cacheRoot)
}

func noCache() bool {
	b, err := strconv.ParseBool(os.Getenv(SigstoreNoCache))
	if err != nil {
		return false
	}
	return b
}

// rootCacheDir returns the cache path to root
func rootCacheDir() string {
	rootDir := os.Getenv(TufRootEnv)
	if rootDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = ""
		}
		return filepath.FromSlash(filepath.Join(home, ".sigstore", "root"))
	}
	return rootDir
}

func cachedRemote(cacheRoot string) string {
	return filepath.FromSlash(filepath.Join(cacheRoot, "map.json"))
}
