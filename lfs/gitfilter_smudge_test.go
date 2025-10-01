package lfs

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/lfsapi"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/stretchr/testify/require"
)

func TestSmudgeStreamsAndCleansTempWhenCacheDisabled(t *testing.T) {
	repoDir := t.TempDir()
	runGit(t, repoDir, "init")
	runGit(t, repoDir, "config", "user.name", "Git LFS Tests")
	runGit(t, repoDir, "config", "user.email", "git-lfs@example.com")
	runGit(t, repoDir, "config", "lfs.storagecache", "false")

	content := []byte("streamed remote content")
	sum := sha256.Sum256(content)
	oid := hex.EncodeToString(sum[:])

	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/objects/batch" {
			var req struct {
				Objects []struct {
					Oid  string `json:"oid"`
					Size int64  `json:"size"`
				} `json:"objects"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&req))
			require.Len(t, req.Objects, 1)
			require.Equal(t, oid, req.Objects[0].Oid)

			resp := map[string]interface{}{
				"transfer": "basic",
				"objects": []map[string]interface{}{
					{
						"oid":  oid,
						"size": req.Objects[0].Size,
						"actions": map[string]map[string]interface{}{
							"download": {
								"href": ts.URL + "/download/" + oid,
							},
						},
					},
				},
			}

			w.Header().Set("Content-Type", "application/vnd.git-lfs+json")
			require.NoError(t, json.NewEncoder(w).Encode(resp))
			return
		}

		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/download/") {
			if strings.TrimPrefix(r.URL.Path, "/download/") != oid {
				http.NotFound(w, r)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(content)
			return
		}

		http.NotFound(w, r)
	}))
	defer ts.Close()

	runGit(t, repoDir, "remote", "add", "origin", ts.URL+"/repo.git")
	runGit(t, repoDir, "config", "remote.lfsdefault", "origin")
	runGit(t, repoDir, "config", "remote.origin.lfsurl", ts.URL)
	runGit(t, repoDir, "config", "lfs.url", ts.URL)
	runGit(t, repoDir, "config", "http.sslverify", "false")

	gitDir := filepath.Join(repoDir, ".git")
	cfg := config.NewIn(repoDir, gitDir)
	require.False(t, cfg.StorageCacheEnabled(), "storage cache should be disabled for this test")

	apiClient, err := lfsapi.NewClient(cfg)
	require.NoError(t, err)
	defer apiClient.Close()

	manifest := tq.NewManifest(cfg.Filesystem(), apiClient, "download", cfg.Remote())
	gf := NewGitFilter(cfg)

	ptr := NewPointer(oid, int64(len(content)), nil)
	workingFile := filepath.Join(repoDir, "remote.bin")

	beforeTemps, err := filepath.Glob(filepath.Join(cfg.TempDir(), "lfs-smudge-*"))
	require.NoError(t, err)
	require.Equal(t, 0, len(beforeTemps))

	var buf bytes.Buffer
	n, err := gf.Smudge(&buf, ptr, workingFile, true, manifest, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, buf.Bytes())

	afterTemps, err := filepath.Glob(filepath.Join(cfg.TempDir(), "lfs-smudge-*"))
	require.NoError(t, err)
	require.Equal(t, 0, len(afterTemps), "temporary smudge artifacts should be removed")

	cachePath, err := gf.ObjectPath(ptr.Oid)
	require.NoError(t, err)
	_, statErr := os.Stat(cachePath)
	require.True(t, os.IsNotExist(statErr), "cache object should not persist when storage cache is disabled")
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "git %s failed: %s", strings.Join(args, " "), string(output))
}
