package lfs

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/git-lfs/git-lfs/v3/config"
	"github.com/git-lfs/git-lfs/v3/errors"
	"github.com/git-lfs/git-lfs/v3/lfsapi"
	"github.com/git-lfs/git-lfs/v3/lfshttp"
	"github.com/git-lfs/git-lfs/v3/tools"
	"github.com/git-lfs/git-lfs/v3/tools/humanize"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/git-lfs/git-lfs/v3/tr"
	"github.com/rubyist/tracerx"
)

func (f *GitFilter) SmudgeToFile(filename string, ptr *Pointer, download bool, manifest tq.Manifest, cb tools.CopyCallback) error {
	tools.MkdirAll(filepath.Dir(filename), f.cfg)

	if stat, _ := os.Stat(filename); stat != nil {
		if ptr.Size == 0 && stat.Size() == 0 {
			return nil
		}

		if stat.Mode()&0200 == 0 {
			if err := os.Chmod(filename, stat.Mode()|0200); err != nil {
				return errors.Wrap(err,
					tr.Tr.Get("Could not restore write permission"))
			}

			// When we're done, return the file back to its normal
			// permission bits.
			defer os.Chmod(filename, stat.Mode())
		}
	}

	abs, err := filepath.Abs(filename)
	if err != nil {
		return errors.New(tr.Tr.Get("could not produce absolute path for %q", filename))
	}

	file, err := os.Create(abs)
	if err != nil {
		return errors.New(tr.Tr.Get("could not create working directory file: %v", err))
	}
	defer file.Close()
	if _, err := f.Smudge(file, ptr, filename, download, manifest, cb); err != nil {
		if errors.IsDownloadDeclinedError(err) {
			// write placeholder data instead
			file.Seek(0, io.SeekStart)
			ptr.Encode(file)
			return err
		} else {
			return errors.New(tr.Tr.Get("could not write working directory file: %v", err))
		}
	}
	return nil
}

func (f *GitFilter) Smudge(writer io.Writer, ptr *Pointer, workingfile string, download bool, manifest tq.Manifest, cb tools.CopyCallback) (int64, error) {
	cacheEnabled := f.cfg.StorageCacheEnabled()
	mediafile := f.cfg.Filesystem().ObjectPathname(ptr.Oid)

	LinkOrCopyFromReference(f.cfg, ptr.Oid, ptr.Size)

	stat, statErr := os.Stat(mediafile)
	if statErr == nil && stat != nil {
		fileSize := stat.Size()
		if fileSize != ptr.Size {
			tracerx.Printf("Removing %s, size %d is invalid", mediafile, fileSize)
			os.RemoveAll(mediafile)
			stat = nil
			statErr = os.ErrNotExist
		}
	}

	if ptr.Size == 0 {
		return 0, nil
	}

	var (
		n   int64
		err error
	)

	if statErr != nil || stat == nil {
		if !download {
			return 0, errors.NewDownloadDeclinedError(statErr, tr.Tr.Get("smudge filter"))
		}

		if cacheEnabled {
			mediafile, err = f.ObjectPath(ptr.Oid)
			if err != nil {
				return 0, err
			}
			n, err = f.downloadFile(writer, ptr, workingfile, mediafile, manifest, cb)
			if err != nil && f.cfg.SearchAllRemotesEnabled() {
				tracerx.Printf("git: smudge: default remote failed. searching alternate remotes")
				n, err = f.downloadFileFallBack(writer, ptr, workingfile, mediafile, manifest, cb)
			}
		} else {
			n, err = f.streamFile(writer, ptr, workingfile, manifest, cb)
			if err != nil && f.cfg.SearchAllRemotesEnabled() {
				tracerx.Printf("git: smudge: default remote failed. searching alternate remotes")
				n, err = f.streamFileFallBack(writer, ptr, workingfile, manifest, cb)
			}
		}
	} else {
		n, err = f.readLocalFile(writer, ptr, mediafile, workingfile, cb)
	}

	if err != nil {
		return 0, errors.NewSmudgeError(err, ptr.Oid, mediafile)
	}

	return n, nil
}

func (f *GitFilter) downloadFile(writer io.Writer, ptr *Pointer, workingfile, mediafile string, manifest tq.Manifest, cb tools.CopyCallback) (int64, error) {
	fmt.Fprintln(os.Stderr, tr.Tr.Get("Downloading %s (%s)", workingfile, humanize.FormatBytes(uint64(ptr.Size))))

	// NOTE: if given, "cb" is a tools.CopyCallback which writes updates
	// to the logpath specified by GIT_LFS_PROGRESS.
	//
	// Either way, forward it into the *tq.TransferQueue so that updates are
	// sent over correctly.

	q := tq.NewTransferQueue(tq.Download, manifest, f.cfg.Remote(),
		tq.WithProgressCallback(cb),
		tq.RemoteRef(f.RemoteRef()),
		tq.WithBatchSize(f.cfg.TransferBatchSize()),
	)
	q.Add(filepath.Base(workingfile), mediafile, ptr.Oid, ptr.Size, false, nil)
	q.Wait()

	if errs := q.Errors(); len(errs) > 0 {
		return 0, errors.Wrap(errors.Join(errs...), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	return f.readLocalFile(writer, ptr, mediafile, workingfile, nil)
}

func (f *GitFilter) downloadFileFallBack(writer io.Writer, ptr *Pointer, workingfile, mediafile string, manifest tq.Manifest, cb tools.CopyCallback) (int64, error) {
	// Attempt to find the LFS objects in all currently registered remotes.
	// When a valid remote is found, this remote is taken persistent for
	// future attempts within downloadFile(). In best case, the ordinary
	// call to downloadFile will then succeed for the rest of files,
	// otherwise this function will again search for a valid remote as fallback.
	remotes := f.cfg.Remotes()
	for index, remote := range remotes {
		q := tq.NewTransferQueue(tq.Download, manifest, remote,
			tq.WithProgressCallback(cb),
			tq.RemoteRef(f.RemoteRef()),
			tq.WithBatchSize(f.cfg.TransferBatchSize()),
		)
		q.Add(filepath.Base(workingfile), mediafile, ptr.Oid, ptr.Size, false, nil)
		q.Wait()

		if errs := q.Errors(); len(errs) > 0 {
			wrappedError := errors.Wrap(errors.Join(errs...), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
			if index >= len(remotes)-1 {
				return 0, wrappedError
			} else {
				tracerx.Printf("git: download: remote failed %s %s", remote, wrappedError)
			}
		} else {
			// Set the remote persistent through all the operation as we found a valid one.
			// This prevents multiple trial and error searches.
			f.cfg.SetRemote(remote)
			return f.readLocalFile(writer, ptr, mediafile, workingfile, nil)
		}
	}
	return 0, errors.Wrap(errors.New(tr.Tr.Get("No known remotes")), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
}

func (f *GitFilter) streamFile(writer io.Writer, ptr *Pointer, workingfile string, manifest tq.Manifest, cb tools.CopyCallback) (int64, error) {
	return f.streamFileFromRemote(writer, ptr, workingfile, manifest, f.cfg.Remote(), cb)
}

func (f *GitFilter) streamFileFallBack(writer io.Writer, ptr *Pointer, workingfile string, manifest tq.Manifest, cb tools.CopyCallback) (int64, error) {
	remotes := f.cfg.Remotes()
	var lastErr error
	for index, remote := range remotes {
		n, err := f.streamFileFromRemote(writer, ptr, workingfile, manifest, remote, cb)
		if err != nil {
			lastErr = err
			if index >= len(remotes)-1 {
				break
			}
			tracerx.Printf("git: download: remote failed %s %s", remote, err)
			continue
		}
		f.cfg.SetRemote(remote)
		return n, nil
	}

	if lastErr != nil {
		return 0, lastErr
	}

	return 0, errors.Wrap(errors.New(tr.Tr.Get("No known remotes")), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
}

func (f *GitFilter) streamFileFromRemote(writer io.Writer, ptr *Pointer, workingfile string, manifest tq.Manifest, remote string, cb tools.CopyCallback) (int64, error) {
	if manifest == nil {
		return 0, errors.New(tr.Tr.Get("cannot download without transfer manifest"))
	}

	api := manifest.APIClient()
	if api == nil {
		return 0, errors.New(tr.Tr.Get("unable to initialize API client"))
	}

	fmt.Fprintln(os.Stderr, tr.Tr.Get("Downloading %s (%s)", workingfile, humanize.FormatBytes(uint64(ptr.Size))))

	batch, err := tq.Batch(manifest, tq.Download, remote, f.RemoteRef(), []*tq.Transfer{
		{Oid: ptr.Oid, Size: ptr.Size, Name: filepath.Base(workingfile)},
	})
	if err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}
	if len(batch.Objects) == 0 {
		return 0, errors.Wrap(errors.New(tr.Tr.Get("Object %s not found on the server.", ptr.Oid)), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	obj := batch.Objects[0]
	if obj.Error != nil {
		return 0, errors.Wrap(obj.Error, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	action, err := obj.Rel("download")
	if err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}
	if action == nil {
		return 0, errors.Wrap(errors.New(tr.Tr.Get("Object %s not found on the server.", ptr.Oid)), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	req, err := f.newDownloadRequest(api, action)
	if err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}
	req = api.LogRequest(req, "lfs.data.download")

	res, err := f.performStreamRequest(api, obj, remote, ptr.Oid, req)
	if err != nil {
		if res != nil {
			res.Body.Close()
		}
		return 0, err
	}
	defer res.Body.Close()

	tempFile, err := os.CreateTemp(f.cfg.TempDir(), fmt.Sprintf("%s-", ptr.Oid))
	if err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}
	tempPath := tempFile.Name()
	defer func() {
		tempFile.Close()
		os.Remove(tempPath)
	}()

	_ = os.Chmod(tempPath, f.cfg.RepositoryPermissions(false))

	reader := tools.NewHashingReader(tools.NewRetriableReader(res.Body))
	if _, err := tools.CopyWithCallback(tempFile, reader, ptr.Size, cb); err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	if err := tempFile.Close(); err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	if oid := reader.Hash(); oid != ptr.Oid {
		return 0, errors.Wrap(errors.New(tr.Tr.Get("expected OID %s, got %s", ptr.Oid, oid)), tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	n, err := f.readLocalFile(writer, ptr, tempPath, workingfile, nil)
	if err != nil {
		return n, errors.Wrap(err, tr.Tr.Get("Error downloading %s (%s)", workingfile, ptr.Oid))
	}

	return n, nil
}

func (f *GitFilter) newDownloadRequest(api *lfsapi.Client, action *tq.Action) (*http.Request, error) {
	href := action.Href
	if api != nil && api.GitEnv().Bool("lfs.transfer.enablehrefrewrite", true) {
		href = api.Endpoints.NewEndpoint(tq.Download.String(), action.Href).Url
	}

	if !strings.HasPrefix(strings.ToLower(href), "http://") && !strings.HasPrefix(strings.ToLower(href), "https://") {
		urlfragment := strings.SplitN(href, "?", 2)[0]
		return nil, errors.New(tr.Tr.Get("missing protocol: %q", urlfragment))
	}

	req, err := http.NewRequest("GET", href, nil)
	if err != nil {
		return nil, err
	}

	for key, value := range action.Header {
		req.Header.Set(key, value)
	}

	return req, nil
}

func (f *GitFilter) performStreamRequest(api *lfsapi.Client, obj *tq.Transfer, remote, oid string, req *http.Request) (*http.Response, error) {
	var (
		res *http.Response
		err error
	)

	if obj.Authenticated {
		res, err = api.Do(req)
	} else {
		endpoint := endpointURLForRequest(req.URL.String(), oid)
		res, err = api.DoWithAuthNoRetry(remote, api.Endpoints.AccessFor(endpoint), req)
	}

	if err != nil {
		if res == nil {
			return nil, errors.NewRetriableError(err)
		}
		if later := errors.NewRetriableLaterError(err, res.Header.Get("Retry-After")); later != nil {
			return res, later
		}
		return res, err
	}

	if res.StatusCode == http.StatusTooManyRequests {
		if later := errors.NewRetriableLaterError(lfshttp.NewStatusCodeError(res), res.Header.Get("Retry-After")); later != nil {
			return res, later
		}
	}

	if res.StatusCode < 200 || res.StatusCode > 299 {
		return res, lfshttp.NewStatusCodeError(res)
	}

	return res, nil
}

func endpointURLForRequest(rawurl, oid string) string {
	parts := strings.Split(rawurl, oid)
	if len(parts) == 0 {
		return rawurl
	}
	return parts[0]
}

func (f *GitFilter) readLocalFile(writer io.Writer, ptr *Pointer, mediafile string, workingfile string, cb tools.CopyCallback) (int64, error) {
	reader, err := tools.RobustOpen(mediafile)
	if err != nil {
		return 0, errors.Wrap(err, tr.Tr.Get("error opening media file"))
	}
	defer reader.Close()

	if ptr.Size == 0 {
		if stat, _ := os.Stat(mediafile); stat != nil {
			ptr.Size = stat.Size()
		}
	}

	if len(ptr.Extensions) > 0 {
		registeredExts := f.cfg.Extensions()
		extensions := make(map[string]config.Extension)
		for _, ptrExt := range ptr.Extensions {
			ext, ok := registeredExts[ptrExt.Name]
			if !ok {
				err := errors.New(tr.Tr.Get("extension '%s' is not configured", ptrExt.Name))
				return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
			}
			ext.Priority = ptrExt.Priority
			extensions[ext.Name] = ext
		}
		exts, err := config.SortExtensions(extensions)
		if err != nil {
			return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
		}

		// pipe extensions in reverse order
		var extsR []config.Extension
		for i := range exts {
			ext := exts[len(exts)-1-i]
			extsR = append(extsR, ext)
		}

		request := &pipeRequest{"smudge", reader, workingfile, extsR}

		response, err := pipeExtensions(f.cfg, request)
		if err != nil {
			return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
		}

		actualExts := make(map[string]*pipeExtResult)
		for _, result := range response.results {
			actualExts[result.name] = result
		}

		// verify name, order, and oids
		oid := response.results[0].oidIn
		if ptr.Oid != oid {
			err = errors.New(tr.Tr.Get("actual OID %s during smudge does not match expected %s", oid, ptr.Oid))
			return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
		}

		for _, expected := range ptr.Extensions {
			actual := actualExts[expected.Name]
			if actual.name != expected.Name {
				err = errors.New(tr.Tr.Get("actual extension name '%s' does not match expected '%s'", actual.name, expected.Name))
				return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
			}
			if actual.oidOut != expected.Oid {
				err = errors.New(tr.Tr.Get("actual OID %s for extension '%s' does not match expected %s", actual.oidOut, expected.Name, expected.Oid))
				return 0, errors.Wrap(err, tr.Tr.Get("smudge filter"))
			}
		}

		// setup reader
		reader, err = os.Open(response.file.Name())
		if err != nil {
			return 0, errors.Wrap(err, tr.Tr.Get("Error opening smudged file: %s", err))
		}
		defer reader.Close()
	}

	n, err := tools.CopyWithCallback(writer, reader, ptr.Size, cb)
	if err != nil {
		return n, errors.Wrap(err, tr.Tr.Get("Error reading from media file: %s", err))
	}

	return n, nil
}
