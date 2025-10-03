package commands

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/git-lfs/git-lfs/v3/errors"
	"github.com/git-lfs/git-lfs/v3/filepathfilter"
	"github.com/git-lfs/git-lfs/v3/git"
	"github.com/git-lfs/git-lfs/v3/lfs"
	"github.com/git-lfs/git-lfs/v3/tq"
	"github.com/git-lfs/git-lfs/v3/tr"
	"github.com/git-lfs/pktline"
	"github.com/rubyist/tracerx"
	"github.com/spf13/cobra"
)

const (
	// cleanFilterBufferCapacity is the desired capacity of the
	// `*git.PacketWriter`'s internal buffer when the filter protocol
	// dictates the "clean" command. 512 bytes is (in most cases) enough to
	// hold an entire LFS pointer in memory.
	cleanFilterBufferCapacity = 512

	// smudgeFilterBufferCapacity is the desired capacity of the
	// `*git.PacketWriter`'s internal buffer when the filter protocol
	// dictates the "smudge" command.
	smudgeFilterBufferCapacity = pktline.MaxPacketLength
)

// filterSmudgeSkip is a command-line flag owned by the `filter-process` command
// dictating whether or not to skip the smudging process, leaving pointers as-is
// in the working tree.
var filterSmudgeSkip bool

type filterTempState struct {
	mu         sync.Mutex
	tempPaths  map[string]string
	waiters    map[string]int
	byPath     map[string]string
	registered map[string]bool
}

func newFilterTempState() *filterTempState {
	return &filterTempState{
		tempPaths:  make(map[string]string),
		waiters:    make(map[string]int),
		byPath:     make(map[string]string),
		registered: make(map[string]bool),
	}
}

func (s *filterTempState) Reserve(ptr *lfs.Pointer, name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if path, ok := s.tempPaths[ptr.Oid]; ok {
		s.waiters[ptr.Oid]++
		s.byPath[name] = ptr.Oid
		return path, nil
	}

	wrapped := &lfs.WrappedPointer{Name: name, Pointer: ptr}
	_, path, _, _, _, err := downloadTransfer(wrapped)
	if err != nil {
		return "", err
	}
	s.tempPaths[ptr.Oid] = path
	s.waiters[ptr.Oid] = 1
	s.byPath[name] = ptr.Oid
	s.registered[ptr.Oid] = false
	return path, nil
}

func (s *filterTempState) TransferReady(oid, path string) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(path) > 0 {
		s.tempPaths[oid] = path
	}
	count := s.waiters[oid]
	if count == 0 || s.registered[oid] {
		return count, false
	}
	s.registered[oid] = true
	return count, true
}

func (s *filterTempState) Release(name string) {
	oid := s.forget(name)
	if len(oid) == 0 {
		return
	}
	path, remove := cfg.Filesystem().ReleaseTempObject(oid)
	if remove && len(path) > 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			tracerx.Printf("git: filter-process: unable to remove temp object %s: %v", path, err)
		}
	}
	s.done(oid)
}

func (s *filterTempState) forget(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	oid := s.byPath[name]
	delete(s.byPath, name)
	return oid
}

func (s *filterTempState) done(oid string) {
	if len(oid) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if count, ok := s.waiters[oid]; ok {
		count--
		if count <= 0 {
			delete(s.waiters, oid)
			delete(s.tempPaths, oid)
			delete(s.registered, oid)
		} else {
			s.waiters[oid] = count
		}
	}
}

func filterCommand(cmd *cobra.Command, args []string) {
	requireStdin(tr.Tr.Get("This command should be run by the Git filter process"))
	setupRepository()
	installHooks(false)

	s := git.NewFilterProcessScanner(os.Stdin, os.Stdout)

	if err := s.Init(); err != nil {
		ExitWithError(err)
	}

	caps, err := s.NegotiateCapabilities()
	if err != nil {
		ExitWithError(err)
	}

	var supportsDelay bool
	for _, cap := range caps {
		if cap == "capability=delay" {
			supportsDelay = true
			break
		}
	}

	skip := filterSmudgeSkip || cfg.Os.Bool("GIT_LFS_SKIP_SMUDGE", false)
	filter := filepathfilter.New(cfg.FetchIncludePaths(), cfg.FetchExcludePaths(), filepathfilter.GitIgnore)

	ptrs := make(map[string]*lfs.Pointer)
	temps := newFilterTempState()

	var q *tq.TransferQueue
	var malformed []string
	var malformedOnWindows []string
	var closeOnce *sync.Once
	var available chan *tq.Transfer
	gitfilter := lfs.NewGitFilter(cfg)
	for s.Scan() {
		var n int64
		var err error
		var delayed bool
		var w *pktline.PktlineWriter

		req := s.Request()

		switch req.Header["command"] {
		case "clean":
			s.WriteStatus(statusFromErr(nil))
			w = pktline.NewPktlineWriter(os.Stdout, cleanFilterBufferCapacity)

			var ptr *lfs.Pointer
			ptr, err = clean(gitfilter, w, req.Payload, req.Header["pathname"], -1)

			if ptr != nil {
				n = ptr.Size
			}
		case "smudge":
			if q == nil && supportsDelay {
				closeOnce = new(sync.Once)
				available = make(chan *tq.Transfer)

				if cfg.AutoDetectRemoteEnabled() {
					// update current remote with information gained by treeish
					newRemote := git.FirstRemoteForTreeish(req.Header["treeish"])
					if newRemote != "" {
						cfg.SetRemote(newRemote)
					}
				}

				q = tq.NewTransferQueue(
					tq.Download,
					getTransferManifestOperationRemote("download", cfg.Remote()),
					cfg.Remote(),
					tq.RemoteRef(currentRemoteRef()),
					tq.WithBatchSize(cfg.TransferBatchSize()),
				)
				go infiniteTransferBuffer(q, available)
			}

			w = pktline.NewPktlineWriter(os.Stdout, smudgeFilterBufferCapacity)
			if req.Header["can-delay"] == "1" {
				var ptr *lfs.Pointer

				n, delayed, ptr, err = delayedSmudge(gitfilter, s, w, req.Payload, q, req.Header["pathname"], skip, filter, temps)

				if delayed {
					ptrs[req.Header["pathname"]] = ptr
				}
			} else {
				s.WriteStatus(statusFromErr(nil))
				from, ferr := incomingOrCached(req.Payload, ptrs[req.Header["pathname"]])
				if ferr != nil {
					break
				}

				n, err = smudge(gitfilter, w, from, req.Header["pathname"], skip, filter)
				if err == nil {
					if !cfg.StorageCacheEnabled() {
						temps.Release(req.Header["pathname"])
					}
					delete(ptrs, req.Header["pathname"])
				}
			}
		case "list_available_blobs":
			closeOnce.Do(func() {
				// The first time that Git sends us the
				// 'list_available_blobs' command, it is given
				// that now it waiting until all delayed blobs
				// are available within this smudge filter call
				//
				// This means that, by the time that we're here,
				// we have seen all entries in the checkout, and
				// should therefore instruct the transfer queue
				// to make a batch out of whatever remaining
				// items it has, and then close itself.
				//
				// This function call is wrapped in a
				// `sync.(*Once).Do()` call so we only call
				// `q.Wait()` once, and is called via a
				// goroutine since `q.Wait()` is blocking.
				go q.Wait()
			})

			// The first, and all subsequent calls to
			// list_available_blobs, we read items from `tq.Watch()`
			// until a read from that channel becomes blocking (in
			// other words, we read until there are no more items
			// immediately ready to be sent back to Git).
			transfers := readAvailable(available, q.BatchSize())
			if !cfg.StorageCacheEnabled() {
				for _, t := range transfers {
					if count, register := temps.TransferReady(t.Oid, t.Path); register && count > 0 {
						cfg.Filesystem().RegisterTempObject(t.Oid, t.Path, count)
					}
				}
			}
			paths := pathnames(transfers)
			if len(paths) == 0 {
				// If `len(paths) == 0`, `tq.Watch()` has
				// closed, indicating that all items have been
				// completely processed, and therefore, sent
				// back to Git for checkout.
				for path, _ := range ptrs {
					// If we sent a path to Git but it
					// didn't ask for the smudge contents,
					// that path is available and Git should
					// accept it later.
					paths = append(paths, fmt.Sprintf("pathname=%s", path))
				}
				// At this point all items have been completely processed,
				// so we explicitly close transfer queue. If Git issues
				// another `smudge` command the transfer queue will be
				// created from scratch. Transfer queue needs to be recreated
				// because it has been already partially closed by `q.Wait()`
				q = nil
			}
			err = s.WriteList(paths)
		default:
			ExitWithError(errors.New(tr.Tr.Get("unknown command %q", req.Header["command"])))
		}

		if errors.IsNotAPointerError(err) {
			malformed = append(malformed, req.Header["pathname"])
			err = nil
		} else if possiblyMalformedObjectSize(n) {
			malformedOnWindows = append(malformedOnWindows, req.Header["pathname"])
		}

		var status git.FilterProcessStatus
		if delayed {
			// If delayed, there is no need to call w.Flush() since
			// no data was written. Calculate the status from the
			// given error using 'delayedStatusFromErr'.
			status = delayedStatusFromErr(err)
		} else if ferr := w.Flush(); ferr != nil {
			// Otherwise, we do need to call w.Flush(), since we
			// have to assume that data was written. If the flush
			// operation was unsuccessful, calculate the status
			// using 'statusFromErr'.
			status = statusFromErr(ferr)
		} else {
			// If the above flush was successful, we calculate the
			// status from the above clean, smudge, or
			// list_available_blobs command using statusFromErr,
			// since we did not delay.
			status = statusFromErr(err)
		}

		s.WriteStatus(status)
	}

	if len(malformed) > 0 {
		fmt.Fprintln(os.Stderr, tr.Tr.GetN(
			"Encountered %d file that should have been a pointer, but wasn't:",
			"Encountered %d files that should have been pointers, but weren't:",
			len(malformed),
			len(malformed),
		))
		for _, m := range malformed {
			fmt.Fprintf(os.Stderr, "\t%s\n", m)
		}
	}

	if len(malformedOnWindows) > 0 && cfg.Git.Bool("lfs.largefilewarning", !git.IsGitVersionAtLeast("2.34.0")) {
		fmt.Fprintln(os.Stderr, tr.Tr.GetN(
			"Encountered %d file that may not have been copied correctly on Windows:",
			"Encountered %d files that may not have been copied correctly on Windows:",
			len(malformedOnWindows),
			len(malformedOnWindows),
		))

		for _, m := range malformedOnWindows {
			fmt.Fprintf(os.Stderr, "\t%s\n", m)
		}

		fmt.Fprint(os.Stderr, "\n", tr.Tr.Get("See: `git lfs help smudge` for more details."), "\n")
	}

	if err := s.Err(); err != nil && err != io.EOF {
		ExitWithError(err)
	}
}

// infiniteTransferBuffer streams the results of q.Watch() into "available" as
// if available had an infinite channel buffer.
func infiniteTransferBuffer(q *tq.TransferQueue, available chan<- *tq.Transfer) {
	// Stream results from q.Watch() into chan "available" via an infinite
	// buffer.

	watch := q.Watch()

	// pending is used to keep track of an ordered list of available
	// `*tq.Transfer`'s that cannot be written to "available" without
	// blocking.
	var pending []*tq.Transfer

	for {
		if len(pending) > 0 {
			select {
			case t, ok := <-watch:
				if !ok {
					// If the list of pending elements is
					// non-empty, stream them out (even if
					// they block), and then close().
					for _, t = range pending {
						available <- t
					}
					close(available)
					return
				}
				pending = append(pending, t)
			case available <- pending[0]:
				// Otherwise, dequeue and shift the first
				// element from pending onto available.
				pending = pending[1:]
			}
		} else {
			t, ok := <-watch
			if !ok {
				// If watch is closed, the "tq" is done, and
				// there are no items on the buffer.  Return
				// immediately.
				close(available)
				return
			}

			select {
			case available <- t:
			// Copy an item directly from <-watch onto available<-.
			default:
				// Otherwise, if that would have blocked, make
				// the new read pending.
				pending = append(pending, t)
			}
		}
	}
}

// incomingOrCached returns an io.Reader that is either the contents of the
// given io.Reader "r", or the encoded contents of "ptr". It returns an error if
// there was an error reading from "r".
//
// This is done because when a `command=smudge` with `can-delay=0` is issued,
// the entry's contents are not sent, and must be re-encoded from the stored
// pointer corresponding to the request's filepath.
func incomingOrCached(r io.Reader, ptr *lfs.Pointer) (io.Reader, error) {
	buf := make([]byte, 1024)
	n, err := r.Read(buf)
	buf = buf[:n]

	if n == 0 {
		if ptr == nil {
			// If we read no data from the given io.Reader "r" _and_
			// there was no data to fall back on, return an empty
			// io.Reader yielding no data.
			return bytes.NewReader(buf), nil
		}
		// If we read no data from the given io.Reader "r", _and_ there
		// is a pointer that we can fall back on, return an io.Reader
		// that yields the encoded version of the given pointer.
		return strings.NewReader(ptr.Encoded()), nil
	}

	if err == io.EOF {
		return bytes.NewReader(buf), nil
	}
	return io.MultiReader(bytes.NewReader(buf), r), err
}

// readAvailable satisfies the accumulation semantics for the
// 'list_available_blobs' command. It accumulates items until:
//
// 1. Reading from the channel of available items blocks, or ...
// 2. There is one item available, or ...
// 3. The 'tq.TransferQueue' is completed.
func readAvailable(ch <-chan *tq.Transfer, cap int) []*tq.Transfer {
	ts := make([]*tq.Transfer, 0, cap)

	for {
		select {
		case t, ok := <-ch:
			if !ok {
				return ts
			}
			ts = append(ts, t)
		default:
			if len(ts) > 0 {
				return ts
			}

			t, ok := <-ch
			if !ok {
				return ts
			}
			return append(ts, t)
		}
	}
}

// pathnames formats a list of *tq.Transfers as a valid response to the
// 'list_available_blobs' command.
func pathnames(ts []*tq.Transfer) []string {
	pathnames := make([]string, 0, len(ts))
	for _, t := range ts {
		pathnames = append(pathnames, fmt.Sprintf("pathname=%s", t.Name))
	}

	return pathnames
}

// statusFromErr returns the status code that should be sent over the filter
// protocol based on a given error, "err".
func statusFromErr(err error) git.FilterProcessStatus {
	if err != nil && err != io.EOF {
		return git.StatusError
	}
	return git.StatusSuccess
}

// delayedStatusFromErr returns the status code that should be sent over the
// filter protocol based on a given error, "err" when the blob smudge operation
// was delayed.
func delayedStatusFromErr(err error) git.FilterProcessStatus {
	status := statusFromErr(err)

	switch status {
	case git.StatusSuccess:
		return git.StatusDelay
	default:
		return status
	}
}

func init() {
	RegisterCommand("filter-process", filterCommand, func(cmd *cobra.Command) {
		cmd.Flags().BoolVarP(&filterSmudgeSkip, "skip", "s", false, "")
	})
}
