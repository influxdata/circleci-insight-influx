package internal

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const defaultHost = "https://circleci.com"

type Fetcher struct {
	token    string
	branch   string
	lookback time.Duration
	hostname string

	client *http.Client

	workflowJobs map[string][]string
}

type FetcherOption func(*Fetcher) error

func NewFetcher(token string, opts ...FetcherOption) (*Fetcher, error) {
	f := &Fetcher{
		token:    token,
		hostname: defaultHost,
		client:   http.DefaultClient, // This may be a custom client later.
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
func (f *Fetcher) Discover(projectSlug string) error {
	// TODO: coerce gh/ at beginning of slug if not formed correctly.

	f.workflowJobs = make(map[string][]string)

	var pageToken string
	var err error
	for {
		pageToken, err = f.discoverWorkflows(projectSlug, pageToken)
		if err != nil {
			return err
		}
		if pageToken == "" {
			break
		}
	}

	for w := range f.workflowJobs {
		for {
			pageToken, err = f.discoverJobs(projectSlug, w, pageToken)
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

func (f *Fetcher) discoverWorkflows(projectSlug, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(projectSlug) + "/workflows"
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

	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverWorkflows: failed to execute request: %w", err)
	}
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

func (f *Fetcher) discoverJobs(projectSlug, workflowName, pageToken string) (string, error) {
	path := "/api/v2/insights/" + url.PathEscape(projectSlug) + "/workflows/" + url.PathEscape(workflowName) + "/jobs"
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

	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("Fetcher.discoverJobs: failed to execute request: %w", err)
	}
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
	return func(f *Fetcher) error {
		f.lookback = lookback
		return nil
	}
}
