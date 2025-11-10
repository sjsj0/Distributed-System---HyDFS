package main // TODO: move this to the level of main.go

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hydfs-g33/config"
	"hydfs-g33/hydfs/cmd/server"
	"hydfs-g33/hydfs/ring"
	"hydfs-g33/hydfs/storage"
	generic_utils "hydfs-g33/utils"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type client struct {
	http *http.Client
	cfg  config.Config
}

const letters = "abcdefghijklmnopqrstuvwxyz"

func randomName(minLen, maxLen int) string {
	length := rand.Intn(maxLen-minLen+1) + minLen
	name := make([]byte, length)
	for i := range name {
		name[i] = letters[rand.Intn(len(letters))]
	}
	return string(name)
}

func newClient(cfg config.Config) *client {
	return &client{
		http: &http.Client{},
		cfg:  cfg,
	}
}

// ---------- helpers ----------

// create a map from "01" to "fa25-cs425-3301.cs.illinois.edu" and so on so forth
var vmAddressMap = map[string]string{
	"01": "fa25-cs425-3301.cs.illinois.edu",
	"02": "fa25-cs425-3302.cs.illinois.edu",
	"03": "fa25-cs425-3303.cs.illinois.edu",
	"04": "fa25-cs425-3304.cs.illinois.edu",
	"05": "fa25-cs425-3305.cs.illinois.edu",
	"06": "fa25-cs425-3306.cs.illinois.edu",
	"07": "fa25-cs425-3307.cs.illinois.edu",
	"08": "fa25-cs425-3308.cs.illinois.edu",
	"09": "fa25-cs425-3309.cs.illinois.edu",
	"10": "fa25-cs425-3310.cs.illinois.edu",
}

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
// POST /v1/user/append?file_name=...&min_replies=...
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
// GET /v1/user/files/content?hydfs_file_name=...&local_file_name=...&min_replies=...
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

// merge
// GET /v1/internal/merge
func (c *client) cmdMerge(hydfsName string) error {
	// Build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/internal/merge?", base)
	if hydfsName != "" {
		u = fmt.Sprintf("%s/v1/internal/merge?hydfs_file_name=%s", base, mustURLEncode(hydfsName))
	} else {
		fmt.Print("merge command requires a HyDFS filename")
	}

	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// Decode server response
	var out struct {
		ReplicaSets map[string][]ring.Node          `json:"replica_sets"`
		Diffs       map[string]server.FileChunkDiff `json:"diffs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(out.ReplicaSets) == 0 && len(out.Diffs) == 0 {
		fmt.Println("merge: no files found on this node")
		return nil
	}

	// Pretty print
	fmt.Println("merge results")
	for fileName, reps := range out.ReplicaSets {
		fmt.Printf("\nfile: %q\n", fileName)
		fmt.Println("replicas:")
		for i, r := range reps {
			fmt.Printf("  [%d] %-20s ring_id=%d\n",
				i,
				generic_utils.ResolveDNSFromIP(r.NodeID.NodeIDToString()),
				r.Token,
			)
		}

		// Per-file diff details
		if diff, ok := out.Diffs[fileName]; ok {
			if len(diff.Errors) > 0 {
				fmt.Println("errors:")
				for nodeStr, msg := range diff.Errors {
					fmt.Printf("  %s: %s\n", generic_utils.ResolveDNSFromIP(nodeStr), msg)
				}
			}

			// Missing chunks by replica
			if len(diff.MissingByNode) > 0 {
				anyMissing := false
				for nodeStr, chunks := range diff.MissingByNode {
					if len(chunks) > 0 {
						if !anyMissing {
							fmt.Println("missing chunks (by replica):")
							anyMissing = true
						}
						fmt.Printf("  %s: %d chunk(s)\n",
							generic_utils.ResolveDNSFromIP(nodeStr), len(chunks))
						// Uncomment to list chunk IDs verbatim:
						// for _, id := range chunks {
						//     fmt.Printf("    - %s\n", id)
						// }
					}
				}
				if !anyMissing {
					fmt.Println("missing chunks: none")
				}
			}
		}
	}

	fmt.Println("\nMerge completed")
	return nil
}

// ls <HyDFSfilename>
// GET /v1/internal/files/ls?hydfs_file_name=...
func (c *client) cmdLs(hydfsName string) error {
	// build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/internal/files/ls?hydfs_file_name=%s",
		base, mustURLEncode(hydfsName))

	// issue GET
	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// decode server response
	var out struct {
		HyDFSFileName string      `json:"hydfs_file_name"`
		FileToken     uint64      `json:"file_token"`
		Replicas      []ring.Node `json:"replicas"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if len(out.Replicas) == 0 {
		fmt.Printf("file %q not found or has no replicas\n", hydfsName)
		return nil
	}

	// print a summary
	fmt.Printf("ls %q\n", out.HyDFSFileName)
	fmt.Printf("file_id=%d\n", out.FileToken)
	fmt.Println("replicas:")
	for i, r := range out.Replicas {
		fmt.Printf("  [%d] %-20s ring_id=%d\n", i, generic_utils.ResolveDNSFromIP(r.NodeID.NodeIDToString()), r.Token)
	}
	return nil
}

// liststore
// GET /v1/internal/files/liststore
func (c *client) cmdListStore() error {
	// build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/internal/files/liststore",
		base)

	// issue GET
	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// decode server response
	var out struct {
		Files         []ring.FileMeta `json:"files"`
		SelfNodeToken uint64          `json:"self_node_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// print a tidy summary
	fmt.Println("Files stored on this node (token =", out.SelfNodeToken, ")")
	if len(out.Files) == 0 {
		fmt.Println("  (no files stored)")
	}
	for _, f := range out.Files {
		fmt.Printf("|- file name: %q, file_id: %d\n", f.Name, f.Token)
	}
	return nil
}

// list_mem_ids
// GET /v1/internal/membership/list_mem_ids
func (c *client) cmdListMemIDs() error {
	// build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/internal/membership/list_mem_ids",
		base)

	// issue GET
	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// decode server response
	var out struct {
		Nodes []ring.Node `json:"nodes"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// print a summary
	fmt.Println("Ring Members (sorted by Ring Token):")
	if len(out.Nodes) == 0 {
		fmt.Println("  (no members found)")
	}
	for i, m := range out.Nodes {
		fmt.Printf("  [%d] Node ID: %-20s, IP Address: %s, Token: %d\n", i, generic_utils.ResolveDNSFromIP(m.NodeID.NodeIDToString()), m.NodeID.NodeIDToString(), m.Token)
	}

	// print a summary
	fmt.Println("Ring Members (sorted by Node Domain Name):")
	if len(out.Nodes) == 0 {
		fmt.Println("  (no members found)")
	}
	sort.Slice(out.Nodes, func(i, j int) bool {
		return generic_utils.ResolveDNSFromIP(out.Nodes[i].NodeID.NodeIDToString()) < generic_utils.ResolveDNSFromIP(out.Nodes[j].NodeID.NodeIDToString())
	})
	for i, m := range out.Nodes {
		fmt.Printf("  [%d] Node ID: %-20s, IP Address: %s, Token: %d\n", i, generic_utils.ResolveDNSFromIP(m.NodeID.NodeIDToString()), m.NodeID.NodeIDToString(), m.Token)
	}
	return nil
}

// getfromreplica <VMAddress> <HyDFSfilename> <localfilename>
// GET /v1/internal/files/content?hydfs_file_name=...&local_file_name=...&min_replies=...
func (c *client) cmdGetFromReplica(vmAddress, hydfsName, localFileName string) error {
	// build request URL
	base := ensureHTTPBase(c.cfg.HydfsHTTP)

	u := fmt.Sprintf("%s/v1/internal/files/getfromreplica?vm_address=%s&hydfs_file_name=%s&local_file_name=%s",
		base, mustURLEncode(vmAddress), mustURLEncode(hydfsName), mustURLEncode(localFileName))

	// issue GET
	resp, err := c.http.Get(u)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}
	// decode server response
	var out struct {
		HyDFSFileName string `json:"hydfs_file_name"`
		LocalFileName string `json:"local_file_name"`
		BytesWritten  int64  `json:"bytes_written"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	// print a tidy summary
	fmt.Printf("getfromreplica %q from %q -> %q\n", hydfsName, vmAddress, localFileName)
	fmt.Printf("  bytes=%d\n", out.BytesWritten)

	return nil
}

// multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilei> <localfilej> ...
// POST /v1/internal/multiappend?min_replies=...
// Body:
//
//	{
//	  "hydfs_file_name": "foo.hydfs",
//	  "pairs": [
//	    {"node_id":"01","local_file":"a.txt"},
//	    {"node_id":"03","local_file":"b.txt"}
//	  ]
//	}
func (c *client) cmdMultiAppend(hydfsName string, NodeIDS []string, localFileNames []string) error {
	// Validate counts.
	if len(NodeIDS) == 0 {
		return fmt.Errorf("no VM codes provided")
	}
	if len(NodeIDS) != len(localFileNames) {
		return fmt.Errorf("mismatch: %d VM codes but %d local files", len(NodeIDS), len(localFileNames))
	}
	if len(NodeIDS) > 10 {
		return fmt.Errorf("too many VMs: %d (max 10)", len(NodeIDS))
	}
	for _, code := range NodeIDS {
		if _, ok := vmAddressMap[code]; !ok {
			return fmt.Errorf("invalid VM code: %q (expected 01..10)", code)
		}
	}

	// Build pairs (map codes -> hostnames).
	pairs := make([]server.MultiAppendPair, len(NodeIDS))
	for i := range NodeIDS {
		pairs[i] = server.MultiAppendPair{
			NodeID:    vmAddressMap[NodeIDS[i]], // server expects hostnames
			LocalFile: localFileNames[i],
		}
	}
	reqBody := server.MultiAppendReq{
		HyDFSFileName: hydfsName,
		Pairs:         pairs,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(&reqBody); err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	minReplies, err := c.minRepliesFor("all")
	if err != nil {
		return err
	}

	base := ensureHTTPBase(c.cfg.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/internal/multiappend?min_replies=%d", base, minReplies)

	req, err := http.NewRequest(http.MethodPost, u, &buf)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	// Read and decode the body regardless of status to surface server errors clearly.
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var mresp server.MultiAppendResp
	if err := json.Unmarshal(body, &mresp); err != nil {
		return fmt.Errorf("decode response: %w; raw=%s", err, string(body))
	}

	// print a brief summary for the REPL UX
	if len(mresp.Errors) > 0 {
		fmt.Printf("multiappend: %d accepted, %d errors\n", mresp.Accepted, len(mresp.Errors))
		for idx, msg := range mresp.Errors {
			// idx is the index into the original pairs slice
			if idx >= 0 && idx < len(pairs) {
				fmt.Printf("  [%d] %s (%s)\n", idx, pairs[idx].NodeID, msg)
			} else {
				fmt.Printf("  [%d] %s\n", idx, msg)
			}
		}
	} else {
		fmt.Printf("multiappend: %d/%d accepted\n", mresp.Accepted, len(pairs))
	}

	return nil
}

func (c *client) cmdMultiCreate(n int, localFileName string) error {

	filesNames := make([]string, n)
	for i := 0; i < n; i++ {
		hydfsFileName := fmt.Sprintf("%s.txt", randomName(4, 8))
		filesNames[i] = hydfsFileName

		fmt.Printf("\n\nCreating HyDFS file %q by uploading local file %q\n", hydfsFileName, localFileName)
		// Use default consistency level "quorum"
		level := "quorum"
		if err := c.cmdCreate(localFileName, hydfsFileName, level); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
		}
	}

	//print all the filesnames created in one line
	fmt.Println("\nCreated HyDFS files:")
	for i, name := range filesNames {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Print(name)
	}
	fmt.Println()
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
		fmt.Print("\n\nhydfs> ")
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
  create <localfilename> <HyDFSfilename>   				- upload and create atomically
  append <localfilename> <HyDFSfilename>   				- upload and append
  get <HyDFSfilename> <localfilename>      				- download to local path
  merge <HyDFSfilename>                       				- merge file replicas
  ls <HyDFSfilename>                          				- list file replicas and file ID
  liststore                                             		- list all files stored on this node
  getfromreplica <VMAddress> <HyDFSfilename> <localfilename> 	- download from specific replica
  list_mem_ids 						 	- list all ring node IDs along with their tokens (sorted by node IDs)
  multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilei> <localfilej> ... - append multiple local files to a HyDFS file
  multicreate n <localfile> ... 				- create n HyDFS files by uploading multiple local files
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

		case "merge":
			// merge <HyDFSfilename>
			if len(args) != 2 {
				fmt.Println("usage: merge <HyDFSfilename>")
				continue
			}
			if err := cli.cmdMerge(args[1]); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "ls":
			//ls HyDFSfilename List all machine (VM) addresses (along with the VMs’ IDs on the ring) where this file is currently being stored. Also prints the fileID of HyDFSfilename.
			// ls <HyDFSfilename>
			if len(args) != 2 {
				fmt.Println("usage: ls <HyDFSfilename>")
				continue
			}
			if err := cli.cmdLs(args[1]); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "liststore":
			//ls HyDFSfilename List all machine (VM) addresses (along with the VMs’ IDs on the ring) where this file is currently being stored. Also prints the fileID of HyDFSfilename.
			// ls <HyDFSfilename>
			if len(args) != 1 {
				fmt.Println("usage: liststore")
				continue
			}
			if err := cli.cmdListStore(); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "list_mem_ids":
			// list_mem_ids: List all ring node IDs along with their tokens (sorted by node IDs)
			if len(args) != 1 {
				fmt.Println("usage: list_mem_ids")
				continue
			}
			if err := cli.cmdListMemIDs(); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "getfromreplica":
			// getfromreplica <VMAddress> <HyDFSfilename> <localfilename>
			if len(args) != 4 {
				fmt.Println("usage: getfromreplica <VMAddress> <HyDFSfilename> <localfilename>")
				continue
			}
			vmAddr := vmAddressMap[args[1]]
			if vmAddr == "" {
				fmt.Println("unknown VM address alias. valid aliases are:")
				for alias := range vmAddressMap {
					fmt.Println(" ", alias)
				}
				continue
			}

			//vmAddr = vmAddr + cli.cfg.HydfsHTTP
			fmt.Printf("Resolved VM address alias %q to %q\n", args[1], vmAddr)
			if err := cli.cmdGetFromReplica(vmAddr, args[2], args[3]); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "multiappend":
			// multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilei> <localfilej> ...
			if len(args) < 4 {
				fmt.Println("usage: multiappend <HyDFSfilename> <VMi> <VMj> ... <localfilei> <localfilej> ...")
				continue
			}

			hydfsName := args[1]
			rest := args[2:]

			// Split rest into NodeIDs (01..10) followed by local file names (same count).
			nodeIDs := make([]string, 0, 10)
			i := 0
			for i < len(rest) {
				if _, ok := vmAddressMap[rest[i]]; !ok {
					break
				}
				nodeIDs = append(nodeIDs, rest[i])
				i++
			}
			localFiles := rest[i:]

			if len(nodeIDs) == 0 || len(localFiles) == 0 || len(nodeIDs) != len(localFiles) || len(nodeIDs) > 10 {
				fmt.Println("error: provide 1–10 VM codes (01..10) followed by the same number of local files")
				continue
			}

			if err := cli.cmdMultiAppend(hydfsName, nodeIDs, localFiles); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "multicreate":
			// multicreate n <localfile>
			if len(args) < 3 {
				fmt.Println("usage: multicreate n <localfile>")
				continue
			}

			//convert n-> arg[1] from string to integer
			n, err := strconv.Atoi(args[1])
			if err != nil || n <= 0 {
				fmt.Println("error: invalid n:", args[1])
				continue
			}
			localFile := args[2]

			fmt.Printf("Creating %d HyDFS files by uploading local file %q\n", n, localFile)
			if err := cli.cmdMultiCreate(n, localFile); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
			}

		case "exit", "quit":
			return

		default:
			fmt.Println("unknown command. type 'help' for usage.")
		}
	}
}
