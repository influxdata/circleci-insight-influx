package internal

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultHost = "https://circleci.com"

type Fetcher struct {
	token         string
	branch        string
	oldestAllowed time.Time
	hostname      string

	client *http.Client

	enc *LineProtocolEncoder

	logger *log.Logger

	workflowJobs map[string][]string
}

type FetcherOption func(*Fetcher) error

func NewFetcher(logger *log.Logger, token string, enc *LineProtocolEncoder, opts ...FetcherOption) (*Fetcher, error) {
	f := &Fetcher{
		token:    token,
		enc:      enc,
		hostname: defaultHost,
		client:   http.DefaultClient, // This may be a custom client later.

		logger: logger,
	}

	for _, o := range opts {
		if err := o(f); err != nil {
			return nil, err
		}
	}

	return f, nil
}

// Discover uses the CircleCI insights API to discover the workflows and jobs within a project.
// projectSlug is expected to be a CircleCI project slug, which doesn't seem to be documented,
// but typically it is "gh/<owner>/<repo>".
func (f *Fetcher) Discover(slug ProjectSlug) error {
	// TODO: coerce gh/ at beginning of slug if not formed correctly.

	f.workflowJobs = make(map[string][]string)

	var pageToken string
	var err error
	for {
		pageToken, err = f.discoverWorkflows(slug, pageToken)
		if err != nil {
			return err
		}
		if pageToken == "" {
			break
		}
	}

	for w := range f.workflowJobs {
		for {
			pageToken, err = f.discoverJobs(slug, w, pageToken)
			if err != nil {
				return err
			}
			if pageToken == "" {
				break
			}
		}
	}

	return nil
}

func (f *Fetcher) discoverWorkflows(slug ProjectSlug, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(slug.String()) + "/workflows"
	sep := "?"
	if pageToken != "" {
		path += sep + "page-token=" + url.QueryEscape(pageToken)
		sep = "&"
	}
	if f.branch != "" {
		path += sep + "branch=" + url.QueryEscape(f.branch)
	}

	req, err := f.request(path)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverWorkflows: failed to create request: %w", err)
	}

	before := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverWorkflows: failed to execute request: %w", err)
	}
	f.enc.FetcherRequest(time.Since(before), "discover_workflows", resp)
	defer resp.Body.Close()

	var workflowResponse paginatedNamedItemResponse

	if err := json.NewDecoder(resp.Body).Decode(&workflowResponse); err != nil {
		return "", fmt.Errorf("Fetcher.discoverWorkflows: failed to decode workflows response with status %d: %w", resp.StatusCode, err)
	}

	for _, i := range workflowResponse.Items {
		f.workflowJobs[i.Name] = nil // Track the workflow.
	}

	return workflowResponse.NextPageToken, nil
}

func (f *Fetcher) discoverJobs(slug ProjectSlug, workflowName, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(slug.String()) + "/workflows/" + url.PathEscape(workflowName) + "/jobs"
	sep := "?"
	if pageToken != "" {
		path += sep + "page-token=" + url.QueryEscape(pageToken)
		sep = "&"
	}
	if f.branch != "" {
		path += sep + "branch=" + url.QueryEscape(f.branch)
	}

	req, err := f.request(path)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverJobs: failed to create request: %w", err)
	}

	before := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverJobs: failed to execute request: %w", err)
	}
	f.enc.FetcherRequest(time.Since(before), "discover_jobs", resp)
	defer resp.Body.Close()

	var jobsResponse paginatedNamedItemResponse

	if err := json.NewDecoder(resp.Body).Decode(&jobsResponse); err != nil {
		return "", fmt.Errorf("Fetcher.discoverJobs: failed to decode jobs response with status %d: %w", resp.StatusCode, err)
	}

	jobNames := make([]string, len(jobsResponse.Items))
	for i, ji := range jobsResponse.Items {
		jobNames[i] = ji.Name
	}
	f.workflowJobs[workflowName] = jobNames

	return jobsResponse.NextPageToken, nil
}

func (f *Fetcher) RecordWorkflowJobMetrics(slug ProjectSlug) (ok bool) {
	ok = true

	var pageToken string
	var err error
	for w, jobs := range f.workflowJobs {
		pageToken, err = f.recordWorkflow(slug, w, pageToken)
		if err != nil {
			f.logger.Printf("Fetcher.RecordWorkflowJobMetrics: failed to record workflow %q: %v", w, err)
			ok = false
			continue // To next workflow.
		}

		for _, j := range jobs {
			for {
				pageToken, err = f.recordJob(slug, w, j, pageToken)
				if err != nil {
					f.logger.Printf("Fetcher.RecordWorkflowJobMetrics: failed to record job %q.%q: %v", w, j, err)
					ok = false
					break // To next job.
				}
				if pageToken == "" {
					break // To next job.
				}
			}
		}
	}

	return ok
}

func (f *Fetcher) recordWorkflow(slug ProjectSlug, w, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(slug.String()) + "/workflows/" + url.PathEscape(w)
	sep := "?"
	if pageToken != "" {
		path += sep + "page-token=" + url.QueryEscape(pageToken)
		sep = "&"
	}
	if f.branch != "" {
		path += sep + "branch=" + url.QueryEscape(f.branch)
	}

	req, err := f.request(path)
	if err != nil {
		return "", fmt.Errorf("Fetcher.recordWorkflow: failed to create request: %w", err)
	}

	before := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.recordWorkflow: failed to execute request: %w", err)
	}
	f.enc.FetcherRequest(time.Since(before), "record_workflow", resp)
	defer resp.Body.Close()

	var workflowResponse paginatedWorkflowMetricItemResponse

	if err := json.NewDecoder(resp.Body).Decode(&workflowResponse); err != nil {
		return "", fmt.Errorf("Fetcher.recordWorkflow: failed to decode workflows response with status %d: %w", resp.StatusCode, err)
	}

	p := WorkflowJobPath{
		VCS:   slug.VCS,
		Owner: slug.Owner,
		Repo:  slug.Repo,

		Branch: f.branch,

		Workflow: w,
	}
	for _, i := range workflowResponse.Items {
		if err := f.enc.WorkflowItem(p, i, f.oldestAllowed); err != nil {
			if _, ok := err.(TooOldError); ok {
				// Stop here, as all further entries will be too old.
				return "", nil
			}
			f.logger.Printf("Fetcher.recordWorkflow: failed to record: %v", err)
			// And keep going anyway.
		}
	}

	return workflowResponse.NextPageToken, nil
}

func (f *Fetcher) recordJob(slug ProjectSlug, w, j, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(slug.String()) + "/workflows/" + url.PathEscape(w) + "/jobs/" + url.PathEscape(j)
	sep := "?"
	if pageToken != "" {
		path += sep + "page-token=" + url.QueryEscape(pageToken)
		sep = "&"
	}
	if f.branch != "" {
		path += sep + "branch=" + url.QueryEscape(f.branch)
	}

	req, err := f.request(path)
	if err != nil {
		return "", fmt.Errorf("Fetcher.recordJob: failed to create request: %w", err)
	}

	before := time.Now()
	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.recordJob: failed to execute request: %w", err)
	}
	f.enc.FetcherRequest(time.Since(before), "record_job", resp)
	defer resp.Body.Close()

	var jobsResponse paginatedJobMetricItemResponse

	if err := json.NewDecoder(resp.Body).Decode(&jobsResponse); err != nil {
		return "", fmt.Errorf("Fetcher.recordJob: failed to decode workflow jobs response with status %d: %w", resp.StatusCode, err)
	}

	p := WorkflowJobPath{
		VCS:   slug.VCS,
		Owner: slug.Owner,
		Repo:  slug.Repo,

		Branch: f.branch,

		Workflow: w,
		Job:      j,
	}
	for _, i := range jobsResponse.Items {
		if err := f.enc.JobItem(p, i, f.oldestAllowed); err != nil {
			if _, ok := err.(TooOldError); ok {
				// Stop here, as all further entries will be too old.
				return "", nil
			}
			f.logger.Printf("Fetcher.recordJob: failed to record: %v", err)
			// And keep going anyway.
		}
	}

	return jobsResponse.NextPageToken, nil
}

// TODO: remove
func (f *Fetcher) DumpWorkflowJobs() {
	for w, js := range f.workflowJobs {
		fmt.Println(w)
		for _, j := range js {
			fmt.Println("\t", j)
		}
	}
}

type paginatedNamedItemResponse struct {
	NextPageToken string `json:"next_page_token"`
	Items         []struct {
		Name string `json:"name"`
	} `json:"items"`
}

type paginatedWorkflowMetricItemResponse struct {
	NextPageToken string         `json:"next_page_token"`
	Items         []WorkflowItem `json:"items"`
}

type paginatedJobMetricItemResponse struct {
	NextPageToken string    `json:"next_page_token"`
	Items         []JobItem `json:"items"`
}

func (f *Fetcher) request(path string) (*http.Request, error) {
	req, err := http.NewRequest("GET", f.hostname+path, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Circle-Token", f.token)
	req.Header.Set("Accept", "application/json") // Not strictly necessary, but that makes the responses have minimal whitespace.
	req.Header.Set("User-Agent", "influxdata/circleci-insight-influx")

	return req, nil
}

func WithHost(hostname string) FetcherOption {
	return func(f *Fetcher) error {
		f.hostname = hostname
		return nil
	}
}

func WithBranch(branch string) FetcherOption {
	return func(f *Fetcher) error {
		f.branch = branch
		return nil
	}
}

func WithLookback(lookback time.Duration) FetcherOption {
	oldest := time.Now().Add(-lookback)
	return func(f *Fetcher) error {
		f.oldestAllowed = oldest
		return nil
	}
}

type ProjectSlug struct {
	VCS, Owner, Repo string
}

func ProjectSlugFromString(s string) (ProjectSlug, error) {
	parts := strings.Split(s, "/")

	if len(parts) != 2 && len(parts) != 3 {
		return ProjectSlug{}, fmt.Errorf("ProjectSlugFromString: invalid string %q; use format [vcs/]owner/repo", s)
	}

	if len(parts) == 2 {
		return ProjectSlug{VCS: "gh", Owner: parts[0], Repo: parts[1]}, nil
	}

	ps := ProjectSlug{VCS: parts[0], Owner: parts[1], Repo: parts[2]}
	if ps.VCS == "github" {
		ps.VCS = "gh" // Normalize.
	}

	return ps, nil
}

func (s ProjectSlug) String() string {
	return s.VCS + "/" + s.Owner + "/" + s.Repo
}
