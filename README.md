# circleci-insight-influx

`circleci-insight-influx` is a small utility that scrapes [the CircleCI Insights API](https://circleci.com/docs/api/v2/#circleci-api-insights)
to discover workflows and jobs,
and then emits line protocol to stdout.

This utility is designed to be run as a [telegraf exec plugin](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/exec/README.md).

Note that the CircleCI summary metrics are updated daily,
so this may mean that it can take up to 24h to discover a newly added workflow or job.
