package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hydfs-g33/config"
	"hydfs-g33/hydfs/ring"
	"hydfs-g33/hydfs/routing"
	"hydfs-g33/hydfs/storage"
	ids "hydfs-g33/hydfs/utils"
	nodeid "hydfs-g33/membership/node"
	generic_utils "hydfs-g33/utils"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// TODO handle seq increment:
// every time a client makes a create request, increment client.seq atomically and under lock

type HTTPServer struct {
	FileStore     *storage.FileStore
	SelfNodeID    nodeid.NodeID // e.g. "http://10.0.0.5:8080"
	Router        *routing.Router
	HTTP          *http.Client // reuse for fan-out; set in daemon
	SelfClientSeq *uint64      // client sequence number for all requests from this node (monotonically increasing)
	Config        config.Config
	RingManager   *ring.Manager
	FileIndex     ring.FileEnumerator
}

func (s *HTTPServer) routes(mux *http.ServeMux) {
	// --- Public (user) endpoints ---
	mux.HandleFunc("POST /v1/user/create_with_data", s.handleUserCreateWithData)
	mux.HandleFunc("POST /v1/user/append", s.handleUserAppend)
	mux.HandleFunc("GET  /v1/user/files/content", s.handleUserGet)

	// --- Replica-only (node→node) endpoints ---
	mux.HandleFunc("POST /v1/replica/create_with_data", s.handleReplicaCreateWithData)
	mux.HandleFunc("POST /v1/replica/append", s.handleReplicaAppend)
	mux.HandleFunc("GET  /v1/replica/files/content", s.handleReplicaGet)
	mux.HandleFunc("GET  /v1/replica/files/manifest", s.handleReplicaManifest)

	// --- Internal (admin) endpoints ---
	mux.HandleFunc("GET /v1/internal/files/fileexists", s.handleFileExists)
	mux.HandleFunc("GET /v1/internal/files/ls", s.handleLs)
	mux.HandleFunc("GET /v1/internal/files/liststore", s.handleListStore)
	mux.HandleFunc("GET /v1/internal/membership/list_mem_ids", s.handleListMemIDs)
	mux.HandleFunc("GET /v1/internal/files/getfromreplica", s.handleGetFromReplica)
	mux.HandleFunc("POST /v1/internal/multiappend", s.handleMultiAppend)
	mux.HandleFunc("GET /v1/internal/merge", s.handleMerge)
}

func (s *HTTPServer) Register(mux *http.ServeMux) { s.routes(mux) }

// ----------------------------------------------------------
// ------------------ INTERNAL HANDLERS ---------------------
// ----------------------------------------------------------

func (s *HTTPServer) handleFileExists(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSfileName := q.Get("hydfs_file_name")
	fmt.Println("\n\n Handle FileExists called with HyDFSfileName:", HyDFSfileName)
	if HyDFSfileName == "" {
		http.Error(w, "hydfs_file_name required", http.StatusBadRequest)
		return
	}
	//check if file exists in HyDFSDir/files/<fileToken> directory
	exists, err := s.FileStore.Paths.FileDirsExist(ids.FileToken64(HyDFSfileName))
	if err != nil {
		http.Error(w, "fileexists: "+err.Error(), http.StatusInternalServerError)
	}
	if exists {
		w.WriteHeader(http.StatusOK)
	}
}

func (s *HTTPServer) handleLs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSfileName := q.Get("hydfs_file_name")

	fmt.Println("\n\n Handle LS called with HyDFSfileName:", HyDFSfileName)

	if HyDFSfileName == "" {
		http.Error(w, "hydfs_file_name required", http.StatusBadRequest)
	}

	//call replicaSet from router to get the nodes information
	replicas := s.Router.ReplicaSet(HyDFSfileName)
	if len(replicas) == 0 {
		http.Error(w, "no replicas available", http.StatusServiceUnavailable)
	}

	//make an empty slice to store the replica only giving response as statusOK when file exists
	var respReplica []ring.Node

	//iterate over replicas and call handleFileExists to check if file exists on each replica, store the responses
	for _, replica := range replicas {
		replicaEndpoint, err := s.resolveReplicaEndpoint(replica.NodeID)
		if err != nil {
			http.Error(w, "resolve replica endpoint: "+err.Error(), http.StatusInternalServerError)
		}
		u := fmt.Sprintf("%s/v1/internal/files/fileexists?hydfs_file_name=%s", replicaEndpoint, url.QueryEscape(HyDFSfileName))
		u = ensureHTTPBase(u)
		fmt.Println("Replica fileexists URL:", u)
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			http.Error(w, "new req: "+err.Error(), http.StatusInternalServerError)
		}
		resp, err := s.HTTP.Do(req)
		if err != nil {
			http.Error(w, "replica fileexists: "+err.Error(), http.StatusInternalServerError)
		}
		defer resp.Body.Close()
		//if resp.StatusCode != http.StatusOK {
		//	http.Error(w, "file does not exist on replica: "+replica.NodeID.NodeIDToString(), http.StatusNotFound)
		//}
		if resp.StatusCode == http.StatusOK {
			respReplica = append(respReplica, replica)
		}

	}

	//get filetoken
	fileToken := ids.FileToken64(HyDFSfileName)

	//put both fileToken and replicas into a struct to send as response
	type LsResponse struct {
		HyDFSFileName string      `json:"hydfs_file_name"`
		FileToken     uint64      `json:"file_token"`
		Replicas      []ring.Node `json:"replicas"`
	}
	lsResp := LsResponse{
		HyDFSFileName: HyDFSfileName,
		FileToken:     fileToken,
		Replicas:      respReplica,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(lsResp)
}

func (s *HTTPServer) handleListStore(w http.ResponseWriter, r *http.Request) {
	fmt.Println("\n\n Handle LISTSTORE called at node:", s.SelfNodeID.NodeIDToString())
	tokenRange := ring.TokenRange{Start: 0, End: ^uint64(0)}
	files, err := s.FileIndex.List(tokenRange)
	selfNodeToken := ids.NodeToken64(s.SelfNodeID)

	if err != nil {
		http.Error(w, "liststore: "+err.Error(), http.StatusInternalServerError)
		return
	}

	type ListStoreResponse struct {
		Files         []ring.FileMeta `json:"files"`
		SelfNodeToken uint64          `json:"self_node_token"`
	}

	response := ListStoreResponse{
		Files:         files,
		SelfNodeToken: selfNodeToken,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *HTTPServer) handleListMemIDs(w http.ResponseWriter, r *http.Request) {
	fmt.Println("\n\n Handle LIST_MEM_IDS called at node:", s.SelfNodeID.NodeIDToString())

	currentRing := s.RingManager.Ring()
	if currentRing == nil {
		http.Error(w, "no ring available", http.StatusServiceUnavailable)
		return
	}

	type ListMemIDsResponse struct {
		Nodes []ring.Node `json:"nodes"`
	}

	nodes := currentRing.Nodes()
	// Sort nodes by Ring token
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Token < nodes[j].Token
	})

	response := ListMemIDsResponse{
		Nodes: nodes,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

func (s *HTTPServer) handleGetFromReplica(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	vmAddress := q.Get("vm_address")
	HyDFSfileName := q.Get("hydfs_file_name")
	LocalFileName := q.Get("local_file_name")

	fmt.Println("\n\n Handle GetFromReplica called with HyDFSfileName:", HyDFSfileName)
	if vmAddress == "" || HyDFSfileName == "" || LocalFileName == "" {
		http.Error(w, "missing query params: vmAddress, hydfs_file_name, local_file_name required", http.StatusBadRequest)
		return
	}

	var (
		bytesWritten int64
	)

	//check if vm_address is self or some other replica
	selfCname := strings.TrimSuffix(generic_utils.ResolveDNSFromIP(s.SelfNodeID.NodeIDToString()), ".")
	fmt.Printf("Self node CNAME: %s\n", selfCname)
	fmt.Printf("Requested vmAddress: %s\n", vmAddress)

	if vmAddress == selfCname {
		//self node, get file locally
		var buf bytes.Buffer
		gr, err := s.streamLocalGet(HyDFSfileName, &buf)
		if err != nil {
			http.Error(w, "get local: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := s.FileStore.CreateLocalFile(LocalFileName, bytes.NewReader(buf.Bytes())); err != nil {
			http.Error(w, "write local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		bytesWritten = gr.Bytes

	} else {
		//remote replica, fetch file from there
		vmAddress = vmAddress + s.Config.HydfsHTTP
		fmt.Printf("Fetching from remote getfromreplica at address: %s\n", vmAddress)

		u := fmt.Sprintf("%s/v1/replica/files/content?hydfs_file_name=%s", vmAddress, url.QueryEscape(HyDFSfileName))
		u = ensureHTTPBase(u)
		fmt.Println("Fetching from chosen replica URL:", u)

		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			http.Error(w, "new req: "+err.Error(), http.StatusInternalServerError)
		}
		resp, err := s.HTTP.Do(req)
		if err != nil {
			http.Error(w, "replica get: "+err.Error(), http.StatusInternalServerError)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			http.Error(w, "replica "+vmAddress+": "+strings.TrimSpace(string(b)), resp.StatusCode)
			return
		}

		// stream to a buffer first so we can hand bytes to createLocalFile
		var buf bytes.Buffer
		n, meta, err := readReplicaMultipart(resp, &buf)
		if err != nil {
			http.Error(w, "multipart read: "+err.Error(), http.StatusBadGateway)
			return
		}
		if err := s.FileStore.CreateLocalFile(LocalFileName, bytes.NewReader(buf.Bytes())); err != nil {
			http.Error(w, "write local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		bytesWritten = n
		// if the replica included JSON meta, you could cross-check version if desired:
		_ = meta // not strictly needed; we trust the quorum-selected manifest
	}
	// Respond with success and bytes written
	type GetFromReplicaResponse struct {
		HyDFSFileName string `json:"hydfs_file_name"`
		LocalFileName string `json:"local_file_name"`
		BytesWritten  int64  `json:"bytes_written"`
	}
	response := GetFromReplicaResponse{
		HyDFSFileName: HyDFSfileName,
		LocalFileName: LocalFileName,
		BytesWritten:  bytesWritten,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

type MultiAppendPair struct {
	NodeID    string `json:"node_id"`
	LocalFile string `json:"local_file"`
}

type MultiAppendReq struct {
	HyDFSFileName string            `json:"hydfs_file_name"`
	Pairs         []MultiAppendPair `json:"pairs"`
}

type MultiAppendResp struct {
	HydfsFileName string            `json:"hydfs_file_name"`
	Accepted      int               `json:"accepted"`
	Errors        map[int]string    `json:"errors,omitempty"` // index -> error
	Pairs         []MultiAppendPair `json:"pairs,omitempty"`  // echoed back if accepted
}

func (s *HTTPServer) handleMultiAppend(w http.ResponseWriter, r *http.Request) {
	fmt.Println("\n\n Handle MULTIAPPEND called at node:", s.SelfNodeID.NodeIDToString())

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// --- parse min_replies (query) ---
	q := r.URL.Query()
	minStr := q.Get("min_replies")
	if minStr == "" {
		http.Error(w, "missing query param: min_replies required", http.StatusBadRequest)
		return
	}
	minReplies := 1
	if n, err := strconv.Atoi(minStr); err == nil && n > 0 {
		minReplies = n
	}

	// --- decode JSON body ---
	var req MultiAppendReq
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// --- basic validation ---
	if strings.TrimSpace(req.HyDFSFileName) == "" {
		http.Error(w, "hydfs_file_name required", http.StatusBadRequest)
		return
	}
	if len(req.Pairs) == 0 {
		http.Error(w, "pairs must be non-empty", http.StatusBadRequest)
		return
	}
	if len(req.Pairs) > 10 {
		http.Error(w, "too many pairs: max 10", http.StatusBadRequest)
		return
	}

	// --- fan out to each node concurrently ---
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		accepted int
		errors   = make(map[int]string) // index -> error string
	)
	wg.Add(len(req.Pairs))

	for idx, p := range req.Pairs {
		idx, p := idx, p // capture loop vars
		go func() {
			defer wg.Done()

			if strings.TrimSpace(p.LocalFile) == "" {
				mu.Lock()
				errors[idx] = "local_file is empty"
				mu.Unlock()
				return
			}

			replicaEndpoint := p.NodeID + s.Config.HydfsHTTP

			// Build URL to that node’s user append endpoint.
			u := fmt.Sprintf("%s/v1/user/append?hydfs_file_name=%s&local_file_name=%s&min_replies=%d",
				replicaEndpoint,
				url.QueryEscape(req.HyDFSFileName),
				url.QueryEscape(p.LocalFile),
				minReplies,
			)
			u = ensureHTTPBase(u)
			fmt.Println("Multiappend → User append URL:", u)

			// POST with empty body (handleUserAppend will read local_file_name on the remote FS)
			httpReq, err := http.NewRequest(http.MethodPost, u, nil)
			if err != nil {
				mu.Lock()
				errors[idx] = "new req: " + err.Error()
				mu.Unlock()
				return
			}
			httpReq.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.HTTP.Do(httpReq)
			if err != nil {
				mu.Lock()
				errors[idx] = err.Error()
				mu.Unlock()
				return
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				mu.Lock()
				errors[idx] = fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
				mu.Unlock()
				return
			}

			// parse user-append JSON to ensure quorum was actually met
			var ua struct {
				Quorum   int `json:"quorum"`
				Received []struct {
					Status int    `json:"status"`
					Err    string `json:"err,omitempty"`
				} `json:"received"`
			}
			if err := json.Unmarshal(body, &ua); err != nil {
				mu.Lock()
				errors[idx] = "decode user-append: " + err.Error()
				mu.Unlock()
				return
			}

			ok := 0
			for _, rcv := range ua.Received {
				if rcv.Status == http.StatusOK {
					ok++
				}
			}
			if ok < ua.Quorum {
				mu.Lock()
				errors[idx] = fmt.Sprintf("append quorum not met: ok=%d quorum=%d", ok, ua.Quorum)
				mu.Unlock()
				return
			}

			// Success for this pair
			mu.Lock()
			accepted++
			mu.Unlock()
		}()
	}

	wg.Wait()

	// --- respond with your typed MultiAppendResp ---
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(MultiAppendResp{
		HydfsFileName: req.HyDFSFileName,
		Accepted:      accepted,
		Errors:        condErrors(errors),
		Pairs:         req.Pairs, // echo original pairs (node_id + local_file)
	})
}

// helper: omit empty "errors" in JSON (ensure nil not empty map)
func condErrors(m map[int]string) map[int]string {
	if len(m) == 0 {
		return nil
	}
	return m
}

type FileChunkDiff struct {
	File           string              `json:"file"`
	GlobalChunkIDs []string            `json:"global_chunk_ids"` // union across replicas
	MissingByNode  map[string][]string `json:"missing_by_node"`  // nodeID -> missing chunkIDs
	Errors         map[string]string   `json:"errors,omitempty"` // nodeID -> error
}

type MergeResponse struct {
	ReplicaSets map[string][]ring.Node   `json:"replica_sets"`
	Diffs       map[string]FileChunkDiff `json:"diffs"`
}

func chunkIDsFromManifest(m *storage.Manifest) []string {
	if m == nil {
		return nil
	}
	var ids []string
	for _, op := range m.Ops {
		for _, chunkID := range op.ChunkIDs {
			ids = append(ids, string(chunkID))
		}
	}
	return ids
}

func toSet(ss []string) map[string]struct{} {
	s := make(map[string]struct{}, len(ss))
	for _, x := range ss {
		s[x] = struct{}{}
	}
	return s
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
func (s *HTTPServer) handleMerge(w http.ResponseWriter, r *http.Request) {
	fmt.Println("\n\n Handle MERGE called at node:", s.SelfNodeID.NodeIDToString())

	// ---- 0) Parse and validate the target file name ----
	hydfsName := r.URL.Query().Get("hydfs_file_name")
	if hydfsName == "" {
		http.Error(w, "merge: missing query param 'hydfs_file_name'", http.StatusBadRequest)
		return
	}

	// ---- 1) Compute replica set for this file ----
	replicas := s.Router.ReplicaSet(hydfsName)
	replicaSets := map[string][]ring.Node{
		hydfsName: replicas,
	}
	if len(replicas) == 0 {
		// Return a response with empty replica set and an error for this file.
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(MergeResponse{
			ReplicaSets: replicaSets,
			Diffs: map[string]FileChunkDiff{
				hydfsName: {
					File:           hydfsName,
					GlobalChunkIDs: nil,
					MissingByNode:  map[string][]string{},
					Errors:         map[string]string{"_all": "no replicas available"},
				},
			},
		})
		return
	}

	// ---- 2) Fetch manifests from all replicas for this file ----
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	type manProbe struct {
		nodeID   nodeid.NodeID
		manifest *storage.Manifest
		err      error
	}

	results := make(chan manProbe, len(replicas))
	for _, rp := range replicas {
		nodeID := rp.NodeID
		go func(addr nodeid.NodeID) {
			m, err := s.fetchManifest(ctx, addr, hydfsName)
			results <- manProbe{nodeID: addr, manifest: m, err: err}
		}(nodeID)
	}

	manByNode := make(map[string]*storage.Manifest, len(replicas))
	errByNode := make(map[string]string)
	for i := 0; i < len(replicas); i++ {
		res := <-results
		key := res.nodeID.NodeIDToString()
		if res.err != nil {
			errByNode[key] = res.err.Error()
			continue
		}
		manByNode[key] = res.manifest
	}

	// ---- 3) Build the union of chunk IDs across all available manifests ----
	union := make(map[string]struct{})
	for _, m := range manByNode {
		for _, cid := range chunkIDsFromManifest(m) {
			union[cid] = struct{}{}
		}
	}
	unionList := keys(union)
	sort.Strings(unionList)

	// ---- 4) Compute missing chunks per replica for this file ----
	missingByNode := make(map[string][]string, len(replicas))
	for _, rp := range replicas {
		key := rp.NodeID.NodeIDToString()
		m := manByNode[key]
		if m == nil {
			// If we couldn't get a manifest from this node, mark all union chunks as missing.
			if len(union) > 0 {
				missingByNode[key] = append([]string(nil), unionList...)
			} else {
				missingByNode[key] = nil
			}
			continue
		}
		have := toSet(chunkIDsFromManifest(m))
		var missing []string
		for cid := range union {
			if _, ok := have[cid]; !ok {
				missing = append(missing, cid)
			}
		}
		sort.Strings(missing)
		missingByNode[key] = missing
	}

	// ---- 5) Respond: replica set + per-replica missing-chunk diff for this file only ----
	diff := FileChunkDiff{
		File:           hydfsName,
		GlobalChunkIDs: unionList,
		MissingByNode:  missingByNode,
		Errors:         errByNode,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(MergeResponse{
		ReplicaSets: replicaSets, // { hydfsName: [...] }
		Diffs:       map[string]FileChunkDiff{hydfsName: diff},
	})
}

// ---------- helpers -----------

func (s *HTTPServer) saveBodyToTemp(r io.Reader) (path string, n int64, _ error) {
	f, err := os.CreateTemp("", "hydfs-body-*")
	if err != nil {
		return "", 0, err
	}
	defer func() {
		if err != nil {
			os.Remove(f.Name())
		}
	}()
	written, err := io.Copy(f, r)
	if err != nil {
		f.Close()
		return "", 0, err
	}
	if err := f.Close(); err != nil {
		return "", 0, err
	}
	return f.Name(), written, nil
}

// Local create using an io.Reader (replica behavior, no fan out).
func (s *HTTPServer) localCreateWithReader(fileName, clientID string, clientSeq uint64, ts time.Time, r io.Reader) (*storage.FileOpResult, error) {
	if createResp, err := s.FileStore.Create(fileName, r, clientID, clientSeq, ts); err != nil {
		return nil, err
	} else {
		return createResp, nil
	}
}

// Public path calls this variant: local create using temp file path.
func (s *HTTPServer) localCreateWithData(fileName, clientID string, clientSeq uint64, ts time.Time, tmpPath string) (*storage.FileOpResult, error) {
	fmt.Printf("localCreateWithData called with tmpPath: %s, fileName: %s\n", tmpPath, fileName)
	f, err := os.Open(tmpPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return s.localCreateWithReader(fileName, clientID, clientSeq, ts, f)
}

// Local append using reader (replica behavior, no fan out).
func (s *HTTPServer) localAppendWithReader(fileName, clientID string, clientSeq uint64, ts time.Time, r io.Reader) (*storage.FileOpResult, error) {
	if appendResp, err := s.FileStore.Append(fileName, r, clientID, clientSeq, ts); err != nil {
		return nil, err
	} else {
		return appendResp, nil
	}
}

func (s *HTTPServer) localAppend(fileName, clientID string, clientSeq uint64, ts time.Time, tmpPath string) (*storage.FileOpResult, error) {
	f, err := os.Open(tmpPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return s.localAppendWithReader(fileName, clientID, clientSeq, ts, f)
}

// shared helper: stream from local store to the provided writer
func (s *HTTPServer) streamLocalGet(fileName string, w io.Writer) (*storage.GetResult, error) {
	return s.FileStore.GetFile(fileName, w)
}

// readReplicaMultipart reads a multipart/mixed response from a replica.
// It copies ONLY the first part (file bytes) into dst, counts those bytes,
// and optionally decodes the second part as JSON GetResult.
//
// Returns (fileBytes, meta, err).
func readReplicaMultipart(resp *http.Response, dst io.Writer) (int64, *storage.GetResult, error) {
	ct := resp.Header.Get("Content-Type")
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil {
		return 0, nil, fmt.Errorf("parse content-type: %w", err)
	}
	if !strings.EqualFold(mt, "multipart/mixed") {
		// allow single-part (legacy) fallback: stream whole body as file
		// remove if you want to enforce multipart only
		n, err := io.Copy(dst, resp.Body)
		return n, nil, err
	}

	mr := multipart.NewReader(resp.Body, params["boundary"])

	// Part 1: file bytes (application/octet-stream)
	p1, err := mr.NextPart()
	if err != nil {
		return 0, nil, fmt.Errorf("read part1: %w", err)
	}
	fileBytes, err := io.Copy(dst, p1)
	_ = p1.Close()
	if err != nil {
		return fileBytes, nil, fmt.Errorf("copy file part: %w", err)
	}

	// Part 2: JSON metadata
	var meta *storage.GetResult
	p2, err := mr.NextPart()
	if err == nil {
		defer p2.Close()
		// Best-effort JSON decode
		var gr storage.GetResult
		if err := json.NewDecoder(p2).Decode(&gr); err == nil {
			meta = &gr
		}
		// ignore decode errors; meta stays nil
	}

	return fileBytes, meta, nil
}

// fetchManifest gets a manifest from a given replica address.
// - For self: load from local storage.
// - For remote: GET /v1/replica/files/manifest?file_name=... (JSON).
func (s *HTTPServer) fetchManifest(ctx context.Context, addr nodeid.NodeID, HyDFSfileName string) (*storage.Manifest, error) {
	fmt.Println("Fetching manifest for", HyDFSfileName, "from", addr.NodeIDToString())
	if addr == s.SelfNodeID {
		m, err := s.FileStore.GetManifest(HyDFSfileName)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, fmt.Errorf("manifest not found locally")
		}
		return m, nil
	}

	replicaEndpoint, err := s.resolveReplicaEndpoint(addr)
	if err != nil {
		err = fmt.Errorf("resolve replica endpoint: %w", err)
		return nil, err
	}
	u := fmt.Sprintf("%s/v1/replica/files/manifest?file_name=%s", replicaEndpoint, url.QueryEscape(HyDFSfileName))
	u = ensureHTTPBase(u)
	fmt.Println("Replica create URL:", u)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("replica %s: %d %s", replicaEndpoint, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var m storage.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (s *HTTPServer) incrementClientSeq() uint64 {
	return atomic.AddUint64(s.SelfClientSeq, 1)
}

func ensureHTTPBase(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return addr
	}
	if strings.HasPrefix(addr, ":") {
		return "http://127.0.0.1" + addr
	}
	return "http://" + addr
}

func (s *HTTPServer) resolveReplicaEndpoint(rp nodeid.NodeID) (string, error) {

	replicaEndpoint := rp.EP.EndpointToString()
	fmt.Println("Resolving replica endpoint for:", replicaEndpoint)

	host, _, err := net.SplitHostPort(replicaEndpoint)
	if err != nil {
		log.Printf("invalid replica endpoint %q: %v", replicaEndpoint, err)
		return "", err
	}
	fmt.Println("Host extracted:", host)

	if s.Config.Env == "dev" {
		port := rp.EP.Port

		_, bindPortStr, err := net.SplitHostPort(s.Config.BindAddr)
		if err != nil {
			log.Printf("invalid bind address %q: %v", s.Config.BindAddr, err)
			return "", err
		}
		bindPort, _ := strconv.Atoi(bindPortStr)

		diff := port - uint16(bindPort)

		hydfsHTTPPort, _ := strconv.Atoi(s.Config.HydfsHTTP[1:]) // skip leading ':'
		fmt.Println("Calculated hydfsHTTPPort:", hydfsHTTPPort)

		replicaEndpoint = fmt.Sprintf("%s:%d", host, hydfsHTTPPort+int(diff))
	} else {
		// assuming s.Config.HydfsHTTP already includes :port or prefix
		replicaEndpoint = fmt.Sprintf("%s%s", host, s.Config.HydfsHTTP)
	}

	fmt.Println("Resolved replica endpoint to:", replicaEndpoint)

	return replicaEndpoint, nil
}

func fileReader(path string) (io.ReadCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// ---------- handlers ----------

// POST /v1/user/create_with_data?hydfs_file_name=...&min_replies=N
func (s *HTTPServer) handleUserCreateWithData(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSFileName := q.Get("hydfs_file_name")
	minStr := q.Get("min_replies")

	fmt.Println("\n\n Handle User CREATE called with HyDFSfileName:", HyDFSFileName, "minStr:", minStr)

	if HyDFSFileName == "" || minStr == "" {
		http.Error(w, "missing query params: hydfs_file_name, min_replies required", http.StatusBadRequest)
		return
	}

	// Buffer body to temp once
	tmp, _, err := s.saveBodyToTemp(r.Body)
	if err != nil {
		http.Error(w, "buffer: "+err.Error(), 500)
		return
	}
	defer os.Remove(tmp)

	// Compute replicas
	var replicas []ring.Node = s.Router.ReplicaSet(HyDFSFileName)
	if len(replicas) == 0 {
		http.Error(w, "no replicas", 503)
		return
	}

	fmt.Println("Replicas for", HyDFSFileName, ":", replicas)

	// quorum
	minReplies := 1
	if minStr != "" {
		if n, err := strconv.Atoi(minStr); err == nil && n > 0 {
			minReplies = n
		}
	}
	if minReplies > len(replicas) {
		minReplies = len(replicas)
	}

	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}

	ch := make(chan reply, len(replicas)) // fully buffered so we don't block after returning

	s.incrementClientSeq()
	clientID := s.SelfNodeID.NodeIDToString()
	timestamp := time.Now()
	// For each replica:
	// Launch one goroutine per replica to do:
	// - if replica == self and self is in set → do local create (no fan out)
	// - else → POST temp file to replica endpoint (no fan out on that side)
	for _, rp := range replicas {
		rp := rp
		go func() {
			// Local create path (replica-only behavior)
			fmt.Println("Replica node ID:", rp.NodeID.NodeIDToString(), "Self node ID:", s.SelfNodeID.NodeIDToString())
			if rp.NodeID == s.SelfNodeID {
				res, err := s.localCreateWithData(HyDFSFileName, clientID, *s.SelfClientSeq, timestamp, tmp)
				if err != nil {
					ch <- reply{Addr: clientID, Status: 0, Err: "local create: " + err.Error(), Self: true}
					return
				}
				ch <- reply{Addr: clientID, Status: http.StatusOK, Result: res, Self: true}
				return
			}

			replicaEndpoint, err := s.resolveReplicaEndpoint(rp.NodeID)
			if err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: 0, Err: "resolve replica endpoint: " + err.Error()}
				return
			}

			// Remote replica call
			u := fmt.Sprintf("%s/v1/replica/create_with_data?hydfs_file_name=%s&client_id=%s&seq=%d&ts_ns=%d",
				replicaEndpoint, url.QueryEscape(HyDFSFileName), url.QueryEscape(clientID), *s.SelfClientSeq, timestamp.UnixNano())
			u = ensureHTTPBase(u)
			fmt.Println("Replica create URL:", u)

			f, err := os.Open(tmp)
			if err != nil {
				http.Error(w, "open tmp: "+err.Error(), 500)
				return
			}
			req, err := http.NewRequest("POST", u, f) // Building the HTTP request
			if err != nil {
				f.Close()
				http.Error(w, "new req: "+err.Error(), 500)
				return
			}

			req.Header.Set("Content-Type", "application/octet-stream")
			resp, err := s.HTTP.Do(req) // Actually send the request
			f.Close()
			if err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: 0, Err: err.Error()}
				return
			}
			defer resp.Body.Close()

			// For create, 200 OK or 409 Conflict (already exists) are acceptable
			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
				b, _ := io.ReadAll(resp.Body)
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Err: strings.TrimSpace(string(b))}
				return
			}
			var res storage.FileOpResult
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Err: "decode: " + err.Error()}
				return
			}
			ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Result: &res}
		}()
	}

	// collect until quorum reached
	results := make([]reply, 0, minReplies)
	for i := 0; i < minReplies; i++ {
		results = append(results, <-ch)
	}

	// respond immediately (do NOT cancel inflight requests)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}{
		Quorum:   minReplies,
		Received: results,
	})
}

// POST /v1/user/append?hydfs_file_name=...&local_file_name=...&min_replies=N
func (s *HTTPServer) handleUserAppend(w http.ResponseWriter, r *http.Request) {

	q := r.URL.Query()
	HyDFSFileName := q.Get("hydfs_file_name")
	localFileName := q.Get("local_file_name")
	minStr := q.Get("min_replies")

	fmt.Println("\n\n Handle User APPEND called with HyDFSfileName:", HyDFSFileName, "minStr:", minStr)

	if HyDFSFileName == "" || minStr == "" {
		http.Error(w, "missing query params: hydfs_file_name, min_replies required", http.StatusBadRequest)
		return
	}

	var tmp string
	var err error
	// If local file given then read from local file system instead of request body
	if localFileName != "" {
		// Read from local file system
		localFilePath := filepath.Join(s.Config.LocalFileDir, localFileName)
		rc, err := fileReader(localFilePath)
		if err != nil {
			http.Error(w, "open local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer rc.Close()
		tmp, _, err = s.saveBodyToTemp(rc)
		if err != nil {
			http.Error(w, "buffer local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		// Buffer body to temp once
		tmp, _, err = s.saveBodyToTemp(r.Body)
		if err != nil {
			http.Error(w, "buffer: "+err.Error(), 500)
			return
		}
	}

	defer os.Remove(tmp)

	// Compute replicas
	var replicas []ring.Node = s.Router.ReplicaSet(HyDFSFileName)
	if len(replicas) == 0 {
		http.Error(w, "no replicas", 503)
		return
	}

	// quorum
	minReplies := 1
	if minStr != "" {
		if n, err := strconv.Atoi(minStr); err == nil && n > 0 {
			minReplies = n
		}
	}
	if minReplies > len(replicas) {
		minReplies = len(replicas)
	}

	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}

	ch := make(chan reply, len(replicas)) // fully buffered

	s.incrementClientSeq()
	clientID := s.SelfNodeID.NodeIDToString()
	timestamp := time.Now()

	// For each replica:
	// - if replica == self and self is in set → do local create (no fan out)
	// - else → POST temp file to replica endpoint (no fan out on that side)
	for _, rp := range replicas {
		rp := rp
		go func() {
			if rp.NodeID == s.SelfNodeID {
				// Local append path (replica-only behavior)
				res, err := s.localAppend(HyDFSFileName, clientID, *s.SelfClientSeq, timestamp, tmp)
				if err != nil {
					ch <- reply{Addr: clientID, Status: 0, Err: "local append: " + err.Error(), Self: true}
					return
				}
				ch <- reply{Addr: clientID, Status: http.StatusOK, Result: res, Self: true}
				return
			}

			replicaEndpoint, err := s.resolveReplicaEndpoint(rp.NodeID)
			if err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: 0, Err: "resolve replica endpoint: " + err.Error()}
				return
			}

			// Remote replica call
			u := fmt.Sprintf("%s/v1/replica/append?hydfs_file_name=%s&client_id=%s&seq=%d&ts_ns=%d",
				replicaEndpoint, url.QueryEscape(HyDFSFileName), url.QueryEscape(clientID), *s.SelfClientSeq, timestamp.UnixNano())
			u = ensureHTTPBase(u)
			fmt.Println("Replica append URL:", u)

			f, err := os.Open(tmp)
			if err != nil {
				http.Error(w, "open tmp: "+err.Error(), 500)
				return
			}
			req, err := http.NewRequest("POST", u, f) // Building the HTTP request
			if err != nil {
				f.Close()
				http.Error(w, "new req: "+err.Error(), 500)
				return
			}
			req.Header.Set("Content-Type", "application/octet-stream")

			resp, err := s.HTTP.Do(req)
			f.Close()
			if err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: 0, Err: err.Error()}
				return
			}
			defer resp.Body.Close()

			// For append, we only treat 200 OK as success
			if resp.StatusCode != http.StatusOK {
				b, _ := io.ReadAll(resp.Body)
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Err: strings.TrimSpace(string(b))}
				return
			}

			var res storage.FileOpResult
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Err: "decode: " + err.Error()}
				return
			}
			ch <- reply{Addr: rp.NodeID.NodeIDToString(), Status: resp.StatusCode, Result: &res}
		}()
	}

	// NEW: collect ALL replies; decide success by count of 200 OK
	received := make([]reply, 0, len(replicas))
	okCount := 0
	for i := 0; i < len(replicas); i++ {
		r := <-ch
		received = append(received, r)
		if r.Status == http.StatusOK {
			okCount++
		}
	}

	success := okCount >= minReplies

	// Return non-200 if quorum not met; include details
	if !success {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}{
		Quorum:   minReplies,
		Received: received,
	})

	// // collect until quorum reached
	// results := make([]reply, 0, minReplies)
	// for i := 0; i < minReplies; i++ {
	// 	results = append(results, <-ch)
	// }

	// // respond immediately; inflight goroutines will finish on their own
	// w.Header().Set("Content-Type", "application/json")
	// _ = json.NewEncoder(w).Encode(struct {
	// 	Quorum   int     `json:"quorum"`
	// 	Received []reply `json:"received"`
	// }{
	// 	Quorum:   minReplies,
	// 	Received: results,
	// })
}

// GET /v1/user/files/content?file_name=...&local_file_name=...&min_replies=N

// Phase 1: concurrently fetch manifests, wait for at least N successes,
// choose the replica with the newest LastUpdate (tie-break by Version).
//
// Phase 2: stream file bytes ONLY from that chosen replica and write to a local file.
func (s *HTTPServer) handleUserGet(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSfileName := q.Get("hydfs_file_name")
	localFileName := q.Get("local_file_name")
	minStr := q.Get("min_replies")

	fmt.Println("\n\n Handle User GET called with HyDFSfileName:", HyDFSfileName, "localFileName:", localFileName, "minStr:", minStr)

	if HyDFSfileName == "" || localFileName == "" {
		http.Error(w, "hydfs_file_name and local_file_name required", http.StatusBadRequest)
		return
	}

	// Discover replicas
	replicas := s.Router.ReplicaSet(HyDFSfileName)
	if len(replicas) == 0 {
		http.Error(w, "no replicas available", http.StatusServiceUnavailable)
		return
	}

	// Parse quorum size
	minReplies := 1
	if minStr != "" {
		if n, err := strconv.Atoi(minStr); err == nil && n > 0 {
			minReplies = n
		}
	}
	if minReplies > len(replicas) {
		minReplies = len(replicas)
	}

	// ---- Phase 1: fetch manifests concurrently until we have quorum ----
	type manProbe struct {
		node     nodeid.NodeID
		Manifest *storage.Manifest
		Err      string
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	results := make(chan manProbe, len(replicas))
	for _, rp := range replicas {
		nodeID := rp.NodeID
		go func(addr nodeid.NodeID) {
			m, err := s.fetchManifest(ctx, addr, HyDFSfileName)
			if err != nil {
				results <- manProbe{node: addr, Err: err.Error()}
				return
			}
			results <- manProbe{node: addr, Manifest: m}
		}(nodeID)
	}

	var (
		okCount      int
		manifestsOK  []manProbe
		manifestsErr []manProbe
	)

	for received := 0; received < len(replicas) && okCount < minReplies; received++ {
		res := <-results
		if res.Manifest != nil {
			manifestsOK = append(manifestsOK, res)
			okCount++
		} else {
			manifestsErr = append(manifestsErr, res)
		}
	}
	// Cancel remaining manifest fetches once quorum met.
	cancel()

	if okCount < minReplies {
		http.Error(w, fmt.Sprintf("manifest quorum not met: got %d/%d", okCount, minReplies), http.StatusBadGateway)
		return
	}

	fmt.Printf("Manifests OK: %+v\n", len(manifestsOK))

	// Choose the manifest with newest LastUpdate (tie-break by Version)
	best := manifestsOK[0]
	for _, mp := range manifestsOK[1:] {
		if mp.Manifest.LastUpdate.After(best.Manifest.LastUpdate) ||
			(mp.Manifest.LastUpdate.Equal(best.Manifest.LastUpdate) && mp.Manifest.Version > best.Manifest.Version) {
			best = mp
		}
	}

	chosenNode := best.node
	chosenVersion := best.Manifest.Version
	chosenLU := best.Manifest.LastUpdate

	fmt.Printf("Chosen manifest from %s: version %d, last update %s\n", chosenNode.NodeIDToString(), chosenVersion, chosenLU)

	// ---- Phase 2: fetch file BYTES only from chosen replica and write local file ----
	var (
		bytesWritten int64
	)

	if chosenNode == s.SelfNodeID {
		// Local: stream into buffer then write local file
		var buf bytes.Buffer
		gr, err := s.streamLocalGet(HyDFSfileName, &buf)
		if err != nil {
			http.Error(w, "get local: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if err := s.FileStore.CreateLocalFile(localFileName, bytes.NewReader(buf.Bytes())); err != nil {
			http.Error(w, "write local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		bytesWritten = gr.Bytes
		// trust chosenVersion from manifest-quorum phase
	} else {
		chosenNodeEndpoint, err := s.resolveReplicaEndpoint(chosenNode)
		if err != nil {
			http.Error(w, "resolve chosen replica endpoint: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Remote: GET multipart/mixed, copy file part into local file
		u := fmt.Sprintf("%s/v1/replica/files/content?hydfs_file_name=%s", chosenNodeEndpoint, url.QueryEscape(HyDFSfileName))
		u = ensureHTTPBase(u)
		fmt.Println("Fetching from chosen replica URL:", u)

		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
		if err != nil {
			http.Error(w, "build request: "+err.Error(), http.StatusInternalServerError)
			return
		}
		resp, err := s.HTTP.Do(req)
		if err != nil {
			http.Error(w, "proxy: "+err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			http.Error(w, fmt.Sprintf("replica %s: %d %s", chosenNodeEndpoint, resp.StatusCode, strings.TrimSpace(string(body))), http.StatusBadGateway)
			return
		}

		// stream to a buffer first so we can hand bytes to createLocalFile
		var buf bytes.Buffer
		n, meta, err := readReplicaMultipart(resp, &buf)
		if err != nil {
			http.Error(w, "multipart read: "+err.Error(), http.StatusBadGateway)
			return
		}
		if err := s.FileStore.CreateLocalFile(localFileName, bytes.NewReader(buf.Bytes())); err != nil {
			http.Error(w, "write local file: "+err.Error(), http.StatusInternalServerError)
			return
		}
		bytesWritten = n
		// if the replica included JSON meta, you could cross-check version if desired:
		_ = meta // not strictly needed; we trust the quorum-selected manifest
	}

	// ---- Respond with metadata about the fetch ----
	type manBrief struct {
		Addr       string    `json:"addr"`
		Version    uint64    `json:"version"`
		LastUpdate time.Time `json:"last_update"`
	}
	type respBody struct {
		FileName       string     `json:"file_name"`
		LocalFileName  string     `json:"local_file_name"`
		FileToken      string     `json:"file_token"`
		ChosenReplica  string     `json:"chosen_replica"`
		ChosenVersion  uint64     `json:"chosen_version"`
		ChosenUpdated  time.Time  `json:"chosen_last_update"`
		Bytes          int64      `json:"bytes_written"`
		Quorum         int        `json:"manifest_quorum"`
		ManifestsUsed  []manBrief `json:"manifests_used"`
		ManifestErrors []string   `json:"manifest_errors,omitempty"`
	}

	used := make([]manBrief, 0, len(manifestsOK))
	for _, mp := range manifestsOK {
		used = append(used, manBrief{
			Addr:       mp.node.NodeIDToString(),
			Version:    mp.Manifest.Version,
			LastUpdate: mp.Manifest.LastUpdate,
		})
	}
	var errs []string
	for _, e := range manifestsErr {
		errs = append(errs, fmt.Sprintf("%s: %s", e.node.NodeIDToString(), e.Err))
	}

	out := respBody{
		FileName:       HyDFSfileName,
		LocalFileName:  localFileName,
		FileToken:      strconv.FormatUint(ids.FileToken64(HyDFSfileName), 10),
		ChosenReplica:  chosenNode.NodeIDToString(),
		ChosenVersion:  chosenVersion,
		ChosenUpdated:  chosenLU,
		Bytes:          bytesWritten,
		Quorum:         minReplies,
		ManifestsUsed:  used,
		ManifestErrors: errs,
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

// POST /v1/replica/create_with_data?hydfs_file_name=...&client_id=...&seq=...&ts_ns=...
func (s *HTTPServer) handleReplicaCreateWithData(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSFileName := q.Get("hydfs_file_name")
	clientID := q.Get("client_id")
	seqStr := q.Get("seq")
	tsStr := q.Get("ts_ns")

	fmt.Println("\n\n Handle Replica CREATE called with HyDFSfileName:", HyDFSFileName)

	fmt.Println("handleReplicaCreateWithData called with HyDFSFileName:", HyDFSFileName, "clientID:", clientID, "seqStr:", seqStr, "tsStr:", tsStr)

	if HyDFSFileName == "" || clientID == "" || seqStr == "" || tsStr == "" {
		http.Error(w, "missing query params: hydfs_file_name, client_id, seq, ts_ns required", http.StatusBadRequest)
		return
	}
	clientSeq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "seq parse: "+err.Error(), http.StatusBadRequest)
		return
	}
	tsNs, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		http.Error(w, "ts_ns parse: "+err.Error(), http.StatusBadRequest)
		return
	}
	ts := time.Unix(0, tsNs)

	res, err := s.localCreateWithReader(HyDFSFileName, clientID, clientSeq, ts, r.Body)
	if err != nil {
		if os.IsExist(err) {
			w.WriteHeader(http.StatusConflict)
			return
		}
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(res)
}

// POST /v1/replica/append?hydfs_file_name=...&client_id=...&seq=...&ts_ns=...
func (s *HTTPServer) handleReplicaAppend(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	HyDFSFileName := q.Get("hydfs_file_name")
	clientID := q.Get("client_id")
	seqStr := q.Get("seq")
	tsStr := q.Get("ts_ns")

	fmt.Println("\n\n Handle Replica APPEND called with HyDFSfileName:", HyDFSFileName, "clientID:", clientID, "seqStr:", seqStr, "tsStr:", tsStr)

	if HyDFSFileName == "" || clientID == "" || seqStr == "" || tsStr == "" {
		http.Error(w, "missing query params: hydfs_file_name, client_id, seq, ts_ns required", http.StatusBadRequest)
		return
	}
	clientSeq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		http.Error(w, "seq parse: "+err.Error(), http.StatusBadRequest)
		return
	}
	tsNs, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		http.Error(w, "ts_ns parse: "+err.Error(), http.StatusBadRequest)
		return
	}
	ts := time.Unix(0, tsNs)

	res, err := s.localAppendWithReader(HyDFSFileName, clientID, clientSeq, ts, r.Body)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(res)
}

// GET /v1/replica/files/manifest?file_name=...
func (s *HTTPServer) handleReplicaManifest(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("file_name")

	fmt.Println("\n\n Handle Replica GET MANIFEST called with HyDFSfileName:", fileName)

	if fileName == "" {
		http.Error(w, "file_name required", http.StatusBadRequest)
		return
	}

	m, err := s.FileStore.GetManifest(fileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "manifest not found", http.StatusNotFound)
			return
		}
		http.Error(w, "get manifest: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if m == nil {
		http.Error(w, "manifest not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(m)
}

// GET /v1/replica/files/content?hydfs_file_name=...
// Local read only; no routing/fanout.
// Streams file bytes + JSON metadata using multipart/mixed.
//
// Part 1: application/octet-stream (file bytes, streamed)
// Part 2: application/json         (GetResult with FileToken, Version, Bytes, ...)
func (s *HTTPServer) handleReplicaGet(w http.ResponseWriter, r *http.Request) {
	fileName := r.URL.Query().Get("hydfs_file_name")

	fmt.Println("\n\n Handle Replica GET FILE called with HyDFSfileName:", fileName)

	if fileName == "" {
		http.Error(w, "hydfs_file_name required", http.StatusBadRequest)
		return
	}

	// Set up multipart writer on the ResponseWriter
	mw := multipart.NewWriter(w)
	w.Header().Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
	// Tip: Do NOT call w.WriteHeader here; letting the first write choose 200 is fine.

	// --- Part 1: file bytes ---
	fileHdr := textproto.MIMEHeader{}
	fileHdr.Set("Content-Type", "application/octet-stream")
	fileHdr.Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"`, filepath.Base(fileName)))

	filePart, err := mw.CreatePart(fileHdr)
	if err != nil {
		http.Error(w, "create multipart file part: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Stream the file into the multipart part
	res, err := s.streamLocalGet(fileName, filePart)
	if err != nil {
		_ = mw.Close() // best effort
		if errors.Is(err, os.ErrNotExist) {
			http.Error(w, "file not found", http.StatusNotFound)
			return
		}
		http.Error(w, "get: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// --- Part 2: JSON metadata ---
	jsonHdr := textproto.MIMEHeader{}
	jsonHdr.Set("Content-Type", "application/json; charset=utf-8")
	jsonPart, err := mw.CreatePart(jsonHdr)
	if err != nil {
		_ = mw.Close()
		http.Error(w, "create multipart json part: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Encode the full GetResult as JSON in the second part
	enc := json.NewEncoder(jsonPart)
	enc.SetIndent("", "") // compact; or remove to avoid any whitespace config
	if err := enc.Encode(res); err != nil {
		_ = mw.Close()
		http.Error(w, "encode json: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Finish the multipart response
	if err := mw.Close(); err != nil {
		http.Error(w, "close multipart: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("replica_get_multipart successful: %s (version: %d, bytes: %d)\n", fileName, res.Version, res.Bytes)
}
