package main // TODO: move this to the level of main.go

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hydfs-g33/config"
	"hydfs-g33/hydfs/storage"
	generic_utils "hydfs-g33/utils"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type client struct {
	http *http.Client
	cfg  config.Config
}

func newClient(cfg config.Config) *client {
	return &client{
		http: &http.Client{},
		cfg:  cfg,
	}
}

// ---------- helpers ----------

func mustURLEncode(s string) string {
	return url.QueryEscape(s)
}

func fileReader(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (c *client) minRepliesFor(level string) (int, error) {
	rf := c.cfg.ReplicationFactor
	if rf <= 0 {
		return 0, fmt.Errorf("invalid replication factor: %d", rf)
	}
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "one":
		return 1, nil
	case "quorum":
		// strict quorum
		return rf/2 + 1, nil
	case "all":
		return rf, nil
	default:
		return 0, fmt.Errorf("invalid consistency level %q (expected one|quorum|all)", level)
	}
}

// ensureHTTPBase prepends http:// if needed.
func ensureHTTPBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	if strings.HasPrefix(addr, ":") {
		return "http://127.0.0.1" + addr
	}
	return "http://" + addr
}

// ---------- commands ----------

// create <localfilename> <HyDFSfilename> <consistencyLevel(one|quorum|all)>
// POST /v1/files/create_with_data?file_name=...&min_replies=...
// body: local file bytes (streamed)
func (c *client) cmdCreate(localFileName, hydfsName, consistencyLevel string) error {
	// Open local file
	localFilePath := filepath.Join(c.cfg.LocalFileDir, localFileName)
	fmt.Print("local file path resolved: " + localFilePath)
	rc, err := fileReader(localFilePath)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer rc.Close()

	// Compute min_replies from consistency level
	minReplies, err := c.minRepliesFor(consistencyLevel)
	if err != nil {
		return err
	}

	// Build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/create_with_data?hydfs_file_name=%s&min_replies=%d",
		base, mustURLEncode(hydfsName), minReplies)

	fmt.Println("\nUploading to", u)

	// Send HTTP POST
	req, err := http.NewRequest(http.MethodPost, u, rc)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// Decode JSON response
	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}
	var out struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// Print result summary
	fmt.Printf("Created %q as %q (min_replies=%d)\n", localFileName, hydfsName, out.Quorum)
	for i, r := range out.Received {
		if r.Result != nil {
			res := r.Result
			fmt.Printf("  [%d] %-20s status=%d self=%v\n", i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self)
			fmt.Printf("       file_token=%s version=%d bytes=%d\n", res.FileToken, res.Version, res.Bytes)
			fmt.Printf("       op_id=%s ts=%s client=%s seq=%d\n",
				res.OpID, res.Timestamp.Format(time.RFC3339Nano), res.ClientID, res.ClientSeq)
		} else {
			fmt.Printf("  [%d] %-20s status=%d self=%v err=%s\n", i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self, r.Err)
		}
	}
	return nil
}

// append <localfilename> <HyDFSfilename> <consistencyLevel(one|quorum|all)>
// POST /v1/append?file_name=...&min_replies=...
// body: local file bytes (streamed)
func (c *client) cmdAppend(localFileName, hydfsName, consistencyLevel string) error {
	// 1) open local file from configured LocalFileDir
	localFilePath := filepath.Join(c.cfg.LocalFileDir, localFileName)
	rc, err := fileReader(localFilePath)
	if err != nil {
		return fmt.Errorf("open local file: %w", err)
	}
	defer rc.Close()

	// 2) compute min_replies from requested consistency level
	minReplies, err := c.minRepliesFor(consistencyLevel)
	if err != nil {
		return err
	}

	// 3) build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/append?hydfs_file_name=%s&min_replies=%d",
		base, mustURLEncode(hydfsName), minReplies)

	// 4) POST the bytes
	req, err := http.NewRequest(http.MethodPost, u, rc)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// 5) decode quorum response
	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}
	var out struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// 6) print a tidy summary
	fmt.Printf("append %q -> %q (min_replies=%d)\n", localFileName, hydfsName, out.Quorum)
	for i, r := range out.Received {
		if r.Result != nil {
			res := r.Result
			fmt.Printf("  [%d] %-20s status=%d self=%v\n", i, r.Addr, r.Status, r.Self)
			fmt.Printf("       file_id=%s version=%d +bytes=%d\n", res.FileToken, res.Version, res.Bytes)
			fmt.Printf("       op_id=%s ts=%s client=%s seq=%d\n",
				res.OpID, res.Timestamp.Format(time.RFC3339Nano), res.ClientID, res.ClientSeq)
		} else {
			fmt.Printf("  [%d] %-20s status=%d self=%v err=%s\n", i, r.Addr, r.Status, r.Self, r.Err)
		}
	}
	return nil
}

// get <HyDFSfilename> <localfilename> [consistency(one|quorum|all)]
// GET /v1/files/content?hydfs_file_name=...&local_file_name=...&min_replies=...
func (c *client) cmdGet(hydfsName, localFileName, consistencyLevel string) error {
	// 1) derive min_replies from consistency level
	level := strings.TrimSpace(strings.ToLower(consistencyLevel))
	if level == "" {
		level = "quorum"
	}
	minReplies, err := c.minRepliesFor(level)
	if err != nil {
		return err
	}

	// 2) build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/files/content?hydfs_file_name=%s&local_file_name=%s&min_replies=%d",
		base, mustURLEncode(hydfsName), mustURLEncode(localFileName), minReplies)

	// 3) issue GET
	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// 4) decode server response
	type manBrief struct {
		Addr       string    `json:"addr"`
		Version    uint64    `json:"version"`
		LastUpdate time.Time `json:"last_update"`
	}
	var out struct {
		FileName       string     `json:"file_name"`
		LocalFileName  string     `json:"local_file_name"`
		FileID         string     `json:"file_id"`
		ChosenReplica  string     `json:"chosen_replica"`
		ChosenVersion  uint64     `json:"chosen_version"`
		ChosenUpdated  time.Time  `json:"chosen_last_update"`
		Bytes          int64      `json:"bytes_written"`
		Quorum         int        `json:"manifest_quorum"`
		ManifestsUsed  []manBrief `json:"manifests_used"`
		ManifestErrors []string   `json:"manifest_errors,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// 5) print a tidy summary
	fmt.Printf("get %q -> %q  (min_replies=%d)\n", hydfsName, localFileName, out.Quorum)
	fmt.Printf("  chosen replica: %s\n", out.ChosenReplica)
	fmt.Printf("  file_id=%s version=%d bytes=%d updated=%s\n",
		out.FileID, out.ChosenVersion, out.Bytes, out.ChosenUpdated.Format(time.RFC3339Nano))

	if len(out.ManifestsUsed) > 0 {
		fmt.Println("  manifests used:")
		for _, m := range out.ManifestsUsed {
			fmt.Printf("    - %s  ver=%d  last_update=%s\n",
				m.Addr, m.Version, m.LastUpdate.Format(time.RFC3339Nano))
		}
	}
	if len(out.ManifestErrors) > 0 {
		fmt.Println("  manifest errors:")
		for _, e := range out.ManifestErrors {
			fmt.Printf("    - %s\n", e)
		}
	}
	return nil
}

// ---------- REPL ----------

func splitArgs(line string) []string {
	var out []string
	var cur strings.Builder
	inQuote := rune(0)
	escape := false
	for _, r := range line {
		switch {
		case escape:
			cur.WriteRune(r)
			escape = false
		case r == '\\':
			escape = true
		case inQuote != 0:
			if r == inQuote {
				inQuote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '"' || r == '\'':
			inQuote = r
		case r == ' ' || r == '\t':
			if cur.Len() > 0 {
				out = append(out, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(r)
		}
	}
	if cur.Len() > 0 {
		out = append(out, cur.String())
	}
	return out
}

func main() {
	// TODO: Derive initial server address from config file
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	cfg, err := config.LoadFromFlags()
	if err != nil {
		log.Fatal(err)
	}

	cli := newClient(cfg)

	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 1024), 1024*1024)

	fmt.Println("hydfsctl (HTTP) REPL")
	fmt.Println("Type 'help' for commands. Ctrl+C or 'exit' to quit.")
	fmt.Println("connected to", cli.cfg.HydfsHTTP)

	for {
		fmt.Print("hydfs> ")
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}

		args := splitArgs(line)
		cmd := strings.ToLower(args[0])

		switch cmd {
		case "help", "?":
			fmt.Println(`
Commands:
  create <localfilename> <HyDFSfilename>   - upload and create atomically
  append <localfilename> <HyDFSfilename>   - upload and append
  get <HyDFSfilename> <localfilename>      - download to local path
  exit | quit
`)

		case "create":
			// allow: create <localFileName> <HyDFSFileName> [consistency]
			if len(args) < 3 || len(args) > 4 {
				fmt.Println("usage: create <localfilename> <HyDFSfilename> [one|quorum|all]")
				continue
			}
			level := "quorum"
			if len(args) == 4 {
				level = args[3]
			}
			if err := cli.cmdCreate(args[1], args[2], level); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "append":
			// append <localfilename> <HyDFSfilename> [one|quorum|all]
			if len(args) < 3 || len(args) > 4 {
				fmt.Println("usage: append <localfilename> <HyDFSfilename> [one|quorum|all]")
				continue
			}
			level := "quorum"
			if len(args) == 4 {
				level = args[3]
			}
			if err := cli.cmdAppend(args[1], args[2], level); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "get":
			// get <HyDFSfilename> <localfilename> [one|quorum|all]
			if len(args) < 3 || len(args) > 4 {
				fmt.Println("usage: get <HyDFSfilename> <localfilename> [one|quorum|all]")
				continue
			}
			level := "quorum"
			if len(args) == 4 {
				level = args[3]
			}
			if err := cli.cmdGet(args[1], args[2], level); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "exit", "quit":
			return

		default:
			fmt.Println("unknown command. type 'help' for usage.")
		}
	}
}
