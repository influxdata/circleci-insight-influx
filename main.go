// Command circleci-insight-influx scrapes the CircleCI Insights API
// and emits line protocol suitable for ingestion into influxdb.
package main

import (
	"flag"
	"fmt"
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
	flag.StringVar(&flags.project, "project", "", "CircleCI project slug, of form 'owner/repo'")
	flag.Parse()

	if flags.project == "" {
		fmt.Fprintln(os.Stderr, "-project is required")
		os.Exit(1)
	}

	env.token = os.Getenv(circleTokenEnvVar)
	if env.token == "" {
		fmt.Fprintln(os.Stderr, circleTokenEnvVar+" must be provided for CircleCI API access")
		os.Exit(1)
	}

	f, err := internal.NewFetcher(
		env.token,
		internal.WithBranch(flags.branch),
		internal.WithLookback(flags.lookback),
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create fetcher: "+err.Error())
		os.Exit(1)
	}

	if err := f.Discover(flags.project); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to discover workflows or jobs: "+err.Error())
		os.Exit(1)
	}

	f.DumpWorkflowJobs()
}
