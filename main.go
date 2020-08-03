// Command circleci-insight-influx scrapes the CircleCI Insights API
// and emits line protocol suitable for ingestion into influxdb.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/influxdata/circleci-insight-influx/internal"
)

var flags struct {
	branch   string
	project  string
	lookback time.Duration
}

var env struct {
	token string
}

const circleTokenEnvVar = "CIRCLECI_TOKEN"

func main() {
	flag.StringVar(&flags.branch, "branch", "", "If provided, only get workflow and job statistics for given branch; otherwise all branches")
	flag.StringVar(&flags.project, "project", "", "CircleCI project slug, of form '[vcs/]owner/repo'")
	flag.DurationVar(&flags.lookback, "lookback", 48*time.Hour, "Exclude metrics on jobs older than this deadline")
	flag.Parse()

	if flags.project == "" {
		fmt.Fprintln(os.Stderr, "-project is required")
		os.Exit(1)
	}

	slug, err := internal.ProjectSlugFromString(flags.project)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	logger := log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds)

	env.token = os.Getenv(circleTokenEnvVar)
	if env.token == "" {
		fmt.Fprintln(os.Stderr, circleTokenEnvVar+" must be provided for CircleCI API access")
		os.Exit(1)
	}

	enc := internal.NewLineProtocolEncoder(logger, os.Stdout)

	f, err := internal.NewFetcher(
		logger,
		env.token,
		enc,
		internal.WithBranch(flags.branch),
		internal.WithLookback(flags.lookback),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create fetcher: "+err.Error())
		os.Exit(1)
	}

	if err := f.Discover(slug); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to discover workflows or jobs: "+err.Error())
		os.Exit(1)
	}

	if !f.RecordWorkflowJobMetrics(slug) {
		// On false, errors have already been logged.
		os.Exit(1)
	}
}
