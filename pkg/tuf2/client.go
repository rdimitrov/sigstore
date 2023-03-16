package tuf2

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rdimitrov/go-tuf-metadata/metadata"
	"github.com/rdimitrov/go-tuf-metadata/metadata/config"
	"github.com/rdimitrov/go-tuf-metadata/metadata/updater"
)

// constants
const (
	// DefaultRemoteRoot is the default remote TUF root location.
	DefaultRemoteRoot = "https://sigstore-tuf-root.storage.googleapis.com"
	// TufRootEnv is the name of the environment variable that locates an alternate local TUF root location.
	TufRootEnv = "TUF_ROOT"
	// SigstoreNoCache is the name of the environment variable that, if set, configures this code to only store root data in memory.
	SigstoreNoCache = "SIGSTORE_NO_CACHE"
)

// global variables
var (
	// singletonTUF holds a single instance of TUF that will get reused on subsequent invocations of initializeTUF
	singletonTUF     *TUF
	singletonTUFOnce = new(sync.Once)
	singletonTUFErr  error

	// initMu locks concurrent calls to initializeTUF
	initMu sync.Mutex

	//go:embed repository
	embeddedRootRepo embed.FS
	getEmbedded      = func() fs.FS { return embeddedRootRepo }
)

// JSON output representing the configured root status
type RootStatus struct {
	Local    string                    `json:"local"`
	Remote   string                    `json:"remote"`
	Metadata map[string]MetadataStatus `json:"metadata"`
	Targets  []string                  `json:"targets"`
}

type MetadataStatus struct {
	Version    int    `json:"version"`
	Size       int    `json:"len"`
	Expiration string `json:"expiration"`
	Error      string `json:"error"`
}

// RemoteCache contains information to cache on the location of the remote
// repository
type remoteCache struct {
	Mirror string `json:"mirror"`
}

// TUF represents a sigstore TUF client
type TUF struct {
	sync.Mutex
	embedded fs.FS
	client   *updater.Updater
	targets  targetImpl
	mirror   string
}

type TargetFile struct {
	Target []byte
	Status StatusKind
}

// Initialize initializes a TUF client
func Initialize(ctx context.Context, mirror string, root []byte) error {
	// Initialize the client. Force an update with remote.
	if _, err := initializeTUF(mirror, root, getEmbedded(), true); err != nil {
		return err
	}
	// Store the remote for later if we are caching.
	if !noCache() {
		remoteInfo := &remoteCache{Mirror: mirror}
		b, err := json.Marshal(remoteInfo)
		if err != nil {
			return err
		}
		if err := os.WriteFile(cachedRemote(rootCacheDir()), b, 0o600); err != nil {
			return fmt.Errorf("storing remote: %w", err)
		}
	}
	return nil
}

func initializeTUF(mirror string, root []byte, embedded fs.FS, forceUpdate bool) (*TUF, error) {
	initMu.Lock()
	defer initMu.Unlock()
	// bootstrap a TUF instance
	singletonTUFOnce.Do(func() {
		t := &TUF{
			mirror:   mirror,
			embedded: embedded,
			targets:  newFileImpl(),
		}
		// get trusted root.json
		rootBytes, err := t.getRoot(root)
		if err != nil {
			singletonTUFErr = fmt.Errorf("getting trusted root: %w", err)
			return
		}
		// configure the TUF client
		cfg, err := config.New(t.mirror, rootBytes)
		if err != nil {
			singletonTUFErr = fmt.Errorf("getting configuration for trusted root: %w", err)
			return
		}
		cfg.LocalMetadataDir = cachedMetadataDir(rootCacheDir())
		cfg.LocalTargetsDir = cachedTargetsDir(rootCacheDir())
		cfg.DisableLocalCache = noCache()

		// create a new TUF client
		t.client, err = updater.New(cfg)
		if err != nil {
			singletonTUFErr = fmt.Errorf("create updater instance: %w", err)
			return
		}
		// persist the TUF instance
		singletonTUF = t
	})
	if singletonTUFErr != nil {
		return nil, singletonTUFErr
	}
	// update (if needed)
	err := singletonTUF.update(forceUpdate)
	if err != nil {
		return nil, err
	}
	return singletonTUF, nil
}

// GetRootStatus gets the current root status for info logging
func GetRootStatus(ctx context.Context) (*RootStatus, error) {
	t, err := NewFromEnv(ctx)
	if err != nil {
		return nil, err
	}
	return t.getRootStatus()
}

// TODO: Remove ctx arg.
func NewFromEnv(_ context.Context) (*TUF, error) {
	// Check for the current remote mirror.
	mirror := getRemoteRoot()
	b, err := os.ReadFile(cachedRemote(rootCacheDir()))
	if err == nil {
		remoteInfo := remoteCache{}
		if err := json.Unmarshal(b, &remoteInfo); err == nil {
			mirror = remoteInfo.Mirror
		}
	}

	// Initializes a new TUF object from the local cache or defaults.
	return initializeTUF(mirror, nil, getEmbedded(), false)
}

func (t *TUF) GetTargetsByMeta(usage UsageKind, fallbacks []string) ([]TargetFile, error) {
	t.Lock()
	targets := t.client.GetTopLevelTargets()
	t.Unlock()
	var matchedTargets []TargetFile
	for name, targetMeta := range targets {
		// Skip any targets that do not include custom metadata.
		if targetMeta.Custom == nil {
			continue
		}
		var scm sigstoreCustomMetadata
		err := json.Unmarshal(*targetMeta.Custom, &scm)
		if err != nil {
			fmt.Fprintf(os.Stderr, "**Warning** Custom metadata not configured properly for target %s, skipping target\n", name)
			continue
		}
		if scm.Sigstore.Usage == usage {
			target, err := t.GetTarget(name)
			if err != nil {
				return nil, fmt.Errorf("error getting target %s by usage: %w", name, err)
			}
			// try searching the cache first, then if corrupted download it
			matchedTargets = append(matchedTargets, TargetFile{Target: target, Status: scm.Sigstore.Status})
		}
	}
	if len(matchedTargets) == 0 {
		for _, fallback := range fallbacks {
			target, err := t.GetTarget(fallback)
			if err != nil {
				fmt.Fprintf(os.Stderr, "**Warning** Missing fallback target %s, skipping\n", fallback)
				continue
			}
			matchedTargets = append(matchedTargets, TargetFile{Target: target, Status: Active})
		}
	}
	if len(matchedTargets) == 0 {
		return matchedTargets, fmt.Errorf("no matching targets by custom metadata, fallbacks not found: %s", strings.Join(fallbacks, ", "))
	}
	return nil, nil
}

func (t *TUF) getRootStatus() (*RootStatus, error) {
	local := rootCacheDir()
	if noCache() {
		local = "in-memory"
	}
	status := &RootStatus{
		Local:    local,
		Remote:   t.mirror,
		Metadata: make(map[string]MetadataStatus),
		Targets:  []string{},
	}

	// Get targets
	for targetName := range t.client.GetTopLevelTargets() {
		status.Targets = append(status.Targets, targetName)
	}

	// Get metadata status
	trustedMeta := t.client.GetTrustedMetadataSet()

	// Get root status
	b, err := trustedMeta.Root.ToBytes(true)
	if err != nil {
		status.Metadata[fmt.Sprintf("%s.json", metadata.ROOT)] = MetadataStatus{Error: err.Error()}
	} else {
		status.Metadata[fmt.Sprintf("%s.json", metadata.ROOT)] = MetadataStatus{
			Version:    int(trustedMeta.Root.Signed.Version),
			Size:       len(b),
			Expiration: trustedMeta.Root.Signed.Expires.Format(time.RFC822),
		}
	}

	// Get Snapshot status
	b, err = trustedMeta.Snapshot.ToBytes(true)
	if err != nil {
		status.Metadata[fmt.Sprintf("%s.json", metadata.SNAPSHOT)] = MetadataStatus{Error: err.Error()}
	} else {
		status.Metadata[fmt.Sprintf("%s.json", metadata.SNAPSHOT)] = MetadataStatus{
			Version:    int(trustedMeta.Snapshot.Signed.Version),
			Size:       len(b),
			Expiration: trustedMeta.Snapshot.Signed.Expires.Format(time.RFC822),
		}
	}

	// Get Timestamp status
	b, err = trustedMeta.Timestamp.ToBytes(true)
	if err != nil {
		status.Metadata[fmt.Sprintf("%s.json", metadata.TIMESTAMP)] = MetadataStatus{Error: err.Error()}
	} else {
		status.Metadata[fmt.Sprintf("%s.json", metadata.TIMESTAMP)] = MetadataStatus{
			Version:    int(trustedMeta.Timestamp.Signed.Version),
			Size:       len(b),
			Expiration: trustedMeta.Timestamp.Signed.Expires.Format(time.RFC822),
		}
	}

	// Get Targets status
	b, err = trustedMeta.Targets[metadata.TARGETS].ToBytes(true)
	if err != nil {
		status.Metadata[fmt.Sprintf("%s.json", metadata.TARGETS)] = MetadataStatus{Error: err.Error()}
	} else {
		status.Metadata[fmt.Sprintf("%s.json", metadata.TARGETS)] = MetadataStatus{
			Version:    int(trustedMeta.Targets[metadata.TARGETS].Signed.Version),
			Size:       len(b),
			Expiration: trustedMeta.Targets[metadata.TARGETS].Signed.Expires.Format(time.RFC822),
		}
	}

	return status, nil
}

func (t *TUF) update(force bool) error {
	// no need to update if we have the latest and valid timestamp or we are not forced to
	if t.client.GetTrustedMetadataSet().Timestamp != nil && !t.client.GetTrustedMetadataSet().Timestamp.Signed.IsExpired(time.Now().UTC()) && !force {
		return nil
	}
	// proceed updating, either forced update or there's no valid timestamp
	// update the TUF metadata
	err := t.client.Refresh()
	if err != nil {
		// ignore metadata.ErrRuntime as it's a result of consecutive Refresh() calls which normally is not an expected behavior
		var tmpErr metadata.ErrRuntime
		if !errors.As(err, &tmpErr) {
			return err
		}
	}
	// sync the target files
	for targetName, targetInfo := range t.client.GetTopLevelTargets() {
		// check if target is already present in cache
		if cachedTargetBytes, err := t.targets.Get(targetName); err == nil {
			// check if the target we have in cache is actually what we expect
			if err = targetInfo.VerifyLengthHashes(cachedTargetBytes); err == nil {
				// target is valid, proceed to the next one
				continue
			}
		}
		// target is not present in cache, so let's try to find it in the embedded store
		rd, ok := t.embedded.(fs.ReadFileFS)
		if !ok {
			return errors.New("fs.ReadFileFS unimplemented for embedded repo")
		}
		targetBytes, err := rd.ReadFile(path.Join("repository", "targets", targetName))
		if err == nil {
			// Unfortunately go:embed appears to somehow replace our line endings on windows, we need to switch them back.
			// It should theoretically be safe to do this everywhere - but the files only seem to get mutated on Windows so
			// let's only change them back there.
			if runtime.GOOS == "windows" {
				targetBytes = bytes.ReplaceAll(targetBytes, []byte("\r\n"), []byte("\n"))
			}
			// check if the target we have in the embedded store is actually what we expect
			if err = targetInfo.VerifyLengthHashes(targetBytes); err == nil {
				// target is valid, persist it in cache
				if err := t.targets.Set(targetName, targetBytes); err != nil {
					return err
				}
				// and proceed to the next one
				continue
			}
		}
		// target is not present in cache nor it is present in the embedded store
		// so let's try to download it
		_, targetBytes, err = singletonTUF.client.DownloadTarget(targetInfo, "", "")
		if err != nil {
			return fmt.Errorf("download target file %s - %w", targetName, err)
		}
		// persist it in cache
		singletonTUF.targets.Set(targetName, targetBytes)
	}
	return nil
}

func (t *TUF) getRoot(root []byte) ([]byte, error) {
	// If the caller does not supply a root, then either use the root in the local directory
	// or default to the embedded one

	// no need to do anything if root.json bytes are a provided
	if root != nil {
		return root, nil
	}

	// see if there's already a root file available locally
	trustedRoot, err := os.ReadFile(filepath.Join(cachedMetadataDir(rootCacheDir()), "root.json"))
	if err == nil {
		return trustedRoot, nil
	}

	// on first initialize, there will be no local root file, so read from the embedded store
	rd, ok := t.embedded.(fs.ReadFileFS)
	if !ok {
		return nil, errors.New("fs.ReadFileFS unimplemented for embedded repo")
	}
	trustedRoot, err = rd.ReadFile(path.Join("repository", "root.json"))
	if err != nil {
		return nil, err
	}
	return trustedRoot, nil
}

func (t *TUF) GetTarget(name string) ([]byte, error) {
	t.Lock()
	defer t.Unlock()
	// Get valid target metadata. Does a local verification.
	validMeta, err := t.client.GetTargetInfo(name)
	if err != nil {
		return nil, fmt.Errorf("error verifying local metadata; local cache may be corrupt: %w", err)
	}
	targetBytes, err := t.targets.Get(name)
	if err != nil {
		return nil, err
	}

	if validMeta.VerifyLengthHashes(targetBytes) != nil {
		return nil, fmt.Errorf("cache contains invalid target; local cache may be corrupt")
	}

	return targetBytes, nil
}
