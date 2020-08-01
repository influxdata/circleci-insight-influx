# circleci-insight-influx

`circleci-insight-influx` is a small utility that scrapes [the CircleCI Insights API](https://circleci.com/docs/api/v2/#circleci-api-insights)
to discover workflows and jobs,
and then emits line protocol to stdout.

This utility is designed to be run as a [telegraf exec plugin](https://github.com/influxdata/telegraf/blob/master/plugins/inputs/exec/README.md).
