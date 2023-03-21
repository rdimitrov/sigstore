package tuf

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
	"github.com/rdimitrov/go-tuf-metadata/metadata/multirepo"
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

// TUF represents a sigstore TUF client
type TUF struct {
	sync.Mutex
	embedded fs.FS
	client   *multirepo.MultiRepoClient
	targets  targetImpl
}

type TargetFile struct {
	Target []byte
	Status StatusKind
}

// Initialize initializes a TUF client
func Initialize(ctx context.Context, mirror string, customMap []byte) error {
	// Initialize the client. Force an update with remote.
	if _, err := initializeTUF(mirror, customMap, getEmbedded(), true); err != nil {
		return err
	}
	return nil
}

func initializeTUF(mirror string, customMap []byte, embedded fs.FS, forceUpdate bool) (*TUF, error) {
	initMu.Lock()
	defer initMu.Unlock()
	// bootstrap a TUF instance
	singletonTUFOnce.Do(func() {
		fmt.Println(" --- Initializing a multi-repository TUF client --- ")
		t := &TUF{
			embedded: embedded,
			targets:  newFileImpl(),
		}
		// prepare the multi-repo initialization
		// get map.json
		mapBytes, err := t.getMap(customMap)
		if err != nil {
			singletonTUFErr = fmt.Errorf("getting map file: %w", err)
			return
		}

		// unmarshal the map file (note: should we expect/support unrecognized values here?)
		var mapFile *multirepo.MultiRepoMapType
		if err := json.Unmarshal(mapBytes, &mapFile); err != nil {
			singletonTUFErr = fmt.Errorf("unmarshal map file: %w", err)
			return
		}

		// collect the trusted root metadata for each repository
		trustedRoots := map[string][]byte{}
		for repo := range mapFile.Repositories {
			bytes, err := t.getRoot(repo)
			if err != nil {
				singletonTUFErr = fmt.Errorf("getting trusted root: %w", err)
				return
			}
			trustedRoots[repo] = bytes
		}
		// configure the TUF client
		cfg, err := multirepo.NewConfig(mapBytes, trustedRoots)
		if err != nil {
			singletonTUFErr = fmt.Errorf("getting configuration for trusted root: %w", err)
			return
		}
		cfg.LocalMetadataDir = cachedMetadataDir(rootCacheDir())
		cfg.LocalTargetsDir = cachedTargetsDir(rootCacheDir())
		cfg.DisableLocalCache = noCache()

		// create a new TUF client
		t.client, err = multirepo.New(cfg)
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
	// Initializes a new TUF object from the local cache or defaults
	return initializeTUF("", nil, getEmbedded(), false)
}

func (t *TUF) GetTargetsByMeta(usage UsageKind, fallbacks []string) ([]TargetFile, error) {
	t.Lock()
	targets, err := t.client.GetTopLevelTargets()
	t.Unlock()
	// get the top-level targets
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("no matching targets by custom metadata, fallbacks not found: %s", strings.Join(fallbacks, ", "))
	}
	return matchedTargets, nil
}

func (t *TUF) getRootStatus() (*RootStatus, error) {
	local := rootCacheDir()
	remote := ""
	if noCache() {
		local = "in-memory"
	}
	for repo, urls := range t.client.Config.RepoMap.Repositories {
		// quite ugly but didn't want to change the struct yet
		remote = fmt.Sprintf("%s %s", fmt.Sprintf("%s: %s", repo, strings.Join(urls, " ")), remote)
	}
	status := &RootStatus{
		Local:    local,
		Remote:   remote,
		Metadata: make(map[string]MetadataStatus),
		Targets:  []string{},
	}

	// Get targets
	targets, err := t.client.GetTopLevelTargets()
	if err != nil {
		return nil, err
	}
	for targetName := range targets {
		status.Targets = append(status.Targets, targetName)
	}

	return status, nil
}

func (t *TUF) update(force bool) error {
	// see if a repository has a missing or an expired timestamp
	timestampExpired := false
	for _, clientTUF := range t.client.TUFClients {
		if clientTUF.GetTrustedMetadataSet().Timestamp != nil {
			if clientTUF.GetTrustedMetadataSet().Timestamp.Signed.IsExpired(time.Now().UTC()) {
				timestampExpired = true
				break
			}
		} else {
			timestampExpired = true
			break
		}
	}
	// no need to update if we have the latest and valid timestamp or we are not forced to
	if !timestampExpired && !force {
		return nil
	}
	fmt.Println(" --- Updating the multi-repository client --- ")
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

	// sync the target files, note that some of the targets for each repo might be missing because
	// we want to have in sync only the targets which comply with the policies listed in the map.json file
	targetFiles, err := t.client.GetTopLevelTargets()
	if err != nil {
		return err
	}

	// loop through all top-level targets (combined list for all repos excluding targets that failed to match the mappings in map.json)
	for targetName := range targetFiles {
		// get the target info and the list of repositories serving this target
		targetInfo, repositories, err := t.client.GetTargetInfo(targetName)
		if err != nil {
			return err
		}
		// check if target is already present in cache
		if cachedTargetBytes, err := t.targets.Get(targetName); err == nil {
			// check if the target we have in cache is actually what we expect
			if err = targetInfo.VerifyLengthHashes(cachedTargetBytes); err == nil {
				// target is valid, proceed to the next one
				fmt.Printf(" --- Target %s verified by a threshold of %d repositories is already present in cache --- \n", targetName, len(repositories))
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
				fmt.Printf(" --- Target %s verified by a threshold of %d repositories loaded from the embedded cache --- \n", targetName, len(repositories))
				// and proceed to the next one
				continue
			}
		}
		// target is not present in cache nor it is present in the embedded store
		// so let's try to download it
		_, targetBytes, err = t.client.DownloadTarget(repositories, targetInfo, "", "")
		if err != nil {
			panic(err)
		}
		if err != nil {
			return fmt.Errorf("download target file %s - %w", targetName, err)
		}
		// persist it in cache
		t.targets.Set(targetName, targetBytes)
		fmt.Printf(" --- Target %s verified by a threshold of %d repositories downloaded from remote --- \n", targetName, len(repositories))

	}
	return nil
}

func (t *TUF) getMap(customMap []byte) ([]byte, error) {
	// return the map file if there was one provided
	if len(customMap) != 0 {
		return customMap, nil
	}

	if !noCache() {
		// see if there's already a map file available locally
		mapFile, err := os.ReadFile(filepath.Join(rootCacheDir(), "map.json"))
		if err == nil {
			return mapFile, nil
		}
	}

	// on first initialize, there will be no local map file, so read from the embedded store
	rd, ok := t.embedded.(fs.ReadFileFS)
	if !ok {
		return nil, errors.New("fs.ReadFileFS unimplemented for embedded repo")
	}
	mapFile, err := rd.ReadFile(path.Join("repository", "map.json"))
	if err != nil {
		return nil, err
	}

	// store the map.json
	if !noCache() {
		if err := os.WriteFile(cachedRemote(rootCacheDir()), mapFile, 0o600); err != nil {
			return nil, fmt.Errorf("storing map.json: %w", err)
		}
	}
	return mapFile, nil
}

func (t *TUF) getRoot(repoName string) ([]byte, error) {
	// build the trusted root metadata name
	rootName := fmt.Sprintf("%s.%s", repoName, "root.json")
	// If the caller does not supply a root, then either use the root in the local directory
	// or default to the embedded one
	if !noCache() {
		// TODO: discuss this point
		// see if there's already a root file available locally
		trustedRoot, err := os.ReadFile(filepath.Join(cachedMetadataDir(rootCacheDir()), rootName))
		if err == nil {
			return trustedRoot, nil
		}
	}

	// on first initialize, there will be no local root file, so read from the embedded store
	rd, ok := t.embedded.(fs.ReadFileFS)
	if !ok {
		return nil, errors.New("fs.ReadFileFS unimplemented for embedded repo")
	}
	trustedRoot, err := rd.ReadFile(path.Join("repository", rootName))
	if err != nil {
		return nil, err
	}
	return trustedRoot, nil
}

func (t *TUF) GetTarget(name string) ([]byte, error) {
	t.Lock()
	defer t.Unlock()
	// Get valid target metadata. Does a local verification.
	validMeta, _, err := t.client.GetTargetInfo(name)
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
