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

// Fetcher interacts with the CircleCI API to retrieve metric details about Workflows and Jobs.
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

// FetcherOption modifies a Fetcher returned by NewFetcher.
type FetcherOption func(*Fetcher) error

// NewFetcher returns an initialized Fetcher.
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
// Typically, a caller will call RecordWorkflowJobMetrics after Discover.
func (f *Fetcher) Discover(slug ProjectSlug) error {
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

// RecordWorkflowJobMetrics retrieves run details for workflows and jobs in the given project.
// It returns true if there were no errors; otherwise, errors are logged and a best effort is made to continue.
func (f *Fetcher) RecordWorkflowJobMetrics(slug ProjectSlug) (ok bool) {
	ok = true

	var pageToken string
	var err error
WORKFLOWS:
	for w, jobs := range f.workflowJobs {
		for {
			pageToken, err = f.recordWorkflow(slug, w, pageToken)
			if err != nil {
				f.logger.Printf("Fetcher.RecordWorkflowJobMetrics: failed to record workflow %q: %v", w, err)
				ok = false
				continue WORKFLOWS
			}
			if pageToken == "" {
				break // Begin processing jobs.
			}
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

// paginatedNamedItemResponse is used when discovering Workflows and Jobs.
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

// WithHost sets the host for the Fetcher.
// If not provided, defaults to https://circleci.com.
func WithHost(hostname string) FetcherOption {
	return func(f *Fetcher) error {
		f.hostname = hostname
		return nil
	}
}

// WithBranch sets which branch to specify when scraping.
// If not set, then will scrape metrics for all aggregated branches.
func WithBranch(branch string) FetcherOption {
	return func(f *Fetcher) error {
		f.branch = branch
		return nil
	}
}

// WithLookback sets the limit on how old of metrics to report.
func WithLookback(lookback time.Duration) FetcherOption {
	oldest := time.Now().Add(-lookback)
	return func(f *Fetcher) error {
		f.oldestAllowed = oldest
		return nil
	}
}

// ProjectSlug is the CircleCI representation of a project.
type ProjectSlug struct {
	VCS, Owner, Repo string
}

// ProjectSlugFromString parses a stringified project slug into a ProjectSlug.
// Valid formats are "vcs/owner/repo", and "owner/repo",
// the latter of which implies vcs of "github".
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

// String returns a stringified version of s.
func (s ProjectSlug) String() string {
	return s.VCS + "/" + s.Owner + "/" + s.Repo
}
