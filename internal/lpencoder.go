package internal

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	protocol "github.com/influxdata/line-protocol"
)

// Special tag value to use when no branch filter was applied.
// This was chosen because branch names are not allowed to start with / in git.
const AnyBranch = "/any"

// LineProtocolEncoder formats domain-specific metrics to line protocol.
type LineProtocolEncoder struct {
	enc    *protocol.Encoder
	logger *log.Logger
}

// NewLineProtocolEncoder returns a new LineProtocolEncoder.
func NewLineProtocolEncoder(logger *log.Logger, out io.Writer) *LineProtocolEncoder {
	enc := protocol.NewEncoder(out)
	enc.SetFieldSortOrder(protocol.SortFields) // Easier for humans to read the output this way.
	return &LineProtocolEncoder{
		logger: logger,
		enc:    enc,
	}
}

// WorkflowJobPath represents the canonical location of a Workflow or Job in CircleCI.
type WorkflowJobPath struct {
	VCS   string
	Owner string
	Repo  string

	// Name of branch used in request.
	// If empty, will be recorded as special value "/all" which is an invalid git branch name.
	Branch string

	// Names of workflow and job.
	// Job is empty if this is just a Workflow path.
	Workflow string
	Job      string
}

// WorkflowItem is the metrics for a single workflow "item" in CircleCI's insights API.
type WorkflowItem struct {
	ID              string `json:"id"`
	CreatedAt       string `json:"created_at"` // Workflow: CreatedAt; Job: StartedAt.
	DurationSeconds int    `json:"duration"`
	Status          string `json:"status"`
	CreditsUsed     int    `json:"credits_used"`
}

// Fields returns the line protocol fields corresponding to i.
func (i WorkflowItem) Fields() map[string]interface{} {
	return map[string]interface{}{
		"id":               i.ID,
		"duration_seconds": i.DurationSeconds,
		"status":           i.Status,
		"credits_used":     i.CreditsUsed,
	}
}

// JobItem is the metrics for a single job "item" in CircleCI's insights API.
type JobItem struct {
	ID              string `json:"id"`
	StartedAt       string `json:"started_at"` // Job: StartedAt; Workflow: CreatedAt.
	DurationSeconds int    `json:"duration"`
	Status          string `json:"status"`
	CreditsUsed     int    `json:"credits_used"`
}

// Fields returns the line protocol fields corresponding to i.
func (i JobItem) Fields() map[string]interface{} {
	return map[string]interface{}{
		"id":               i.ID,
		"duration_seconds": i.DurationSeconds,
		"status":           i.Status,
		"credits_used":     i.CreditsUsed,
	}
}

// TooOldError is returned when iterating through a slice of WorkItems or JobItems
// and an item older than the Fetcher's Lookback is encountered.
type TooOldError struct {
	Time time.Time
}

var _ error = TooOldError{}

func (e TooOldError) Error() string {
	return fmt.Sprintf("entry with time %s is older than allowed", e.Time.UTC().Format(time.RFC3339))
}

// WorkflowItem encodes the given WorkflowItem to line protocol.
func (e *LineProtocolEncoder) WorkflowItem(p WorkflowJobPath, i WorkflowItem, oldestAllowed time.Time) error {
	ts, err := time.Parse(time.RFC3339, i.CreatedAt)
	if err != nil {
		return fmt.Errorf("LineProtocolEncoder.WorkflowItem: failed to parse timestamp %q: %w", i.CreatedAt, err)
	}

	if ts.Before(oldestAllowed) {
		return TooOldError{Time: ts}
	}

	branch := p.Branch
	if branch == "" {
		branch = AnyBranch
	}

	m, err := protocol.New(
		"workflow",
		map[string]string{
			"vcs":           p.VCS,
			"project":       p.Owner + "/" + p.Repo,
			"workflow_name": p.Workflow,
			"branch":        branch,
		},
		i.Fields(),
		ts,
	)
	if err != nil {
		return fmt.Errorf("LineProtocolEncoder.WorkflowItem: error constructing point: %w", err)
	}

	if _, err := e.enc.Encode(m); err != nil {
		return fmt.Errorf("LineProtocolEncoder.WorkflowItem: error encoding point: %w", err)
	}

	return nil
}

// JobItem encodes the given JobItem to line protocol.
func (e *LineProtocolEncoder) JobItem(p WorkflowJobPath, i JobItem, oldestAllowed time.Time) error {
	ts, err := time.Parse(time.RFC3339, i.StartedAt)
	if err != nil {
		return fmt.Errorf("LineProtocolEncoder.JobItem: failed to parse timestamp %q: %w", i.StartedAt, err)
	}

	if ts.Before(oldestAllowed) {
		return TooOldError{Time: ts}
	}

	branch := p.Branch
	if branch == "" {
		branch = AnyBranch
	}

	m, err := protocol.New(
		"job",
		map[string]string{
			"vcs":           p.VCS,
			"project":       p.Owner + "/" + p.Repo,
			"workflow_name": p.Workflow,
			"job_name":      p.Job,
			"branch":        branch,
		},
		i.Fields(),
		ts,
	)
	if err != nil {
		return fmt.Errorf("LineProtocolEncoder.JobItem: error encoding point: %w", err)
	}

	_, err = e.enc.Encode(m)
	if err != nil {
		return fmt.Errorf("LineProtocolEncoder.JobItem: error encoding point: %w", err)
	}

	return nil
}

// FetcherRequest records internal metrics for an HTTP request that the Fetcher made.
func (e *LineProtocolEncoder) FetcherRequest(d time.Duration, typ string, resp *http.Response) {
	ts := time.Now()

	fields := map[string]interface{}{
		"duration_ms": d.Milliseconds(),
		"path":        resp.Request.URL.Path,
	}
	ratelimitRemaining := resp.Header.Get("X-Ratelimit-Remaining")
	if n, err := strconv.Atoi(ratelimitRemaining); err != nil {
		e.logger.Printf("LineProtocolEncoder.FetcherRequest: could not parse remaining rate limit %q: %v", ratelimitRemaining, err)
	} else {
		fields["ratelimit_remaining"] = n
	}

	m, err := protocol.New(
		"cii_internal",
		map[string]string{
			"type": typ,
		},
		fields,
		ts,
	)
	if err != nil {
		e.logger.Printf("LineProtocolEncoder.FetcherRequest: error constructing point: %v", err)
		return
	}

	if _, err := e.enc.Encode(m); err != nil {
		e.logger.Printf("LineProtocolEncoder.FetcherRequest: error encoding point: %v", err)
		return
	}
}
