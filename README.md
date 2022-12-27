
# Overview

Trino Query SLS is a [Trino (formerly Presto SQL)](https://trino.io/) plugin for logging query events into aliyun sls (fork trino-querylog).

Its main purpose is to gather queries metadata and statistics as one event per line, so it can be easily collected by external software (e.g. Elastic FileBeat which will send data to Logstash/ElasticSearch/Kibana for storage/analysis).


## Build

```
mvn clean package dependency:copy-dependencies -DincludeScope=runtime
```

## Deploy

### Copy artifacts

Copy the following artifacts (after successful build) to the Trino plugin folder (`<path_to_trino>/plugin/trino-query-sls/`)
```
target/dependency/*.jar
target/trino-query-sls-*.jar
```

### Prepare configuration file

Create `<path_to_trino>/etc/event-listener.properties` with the following required parameters, e.g.:

```
event-listener.name=trino-query-sls
event-listener.query.sls.project=aliyun sls project name
event-listener.query.sls.endpoint=aliyun sls endpoint
event-listener.query.sls.accessKeyId=aliyun sls accessKeyId
event-listener.query.sls.accessKeySecret=aliyun sls accessKeySecret

# must be first create log_store in aliyun sls
event-listener.query.sls.queryCreatedName=aliyun sls log_store name, eg: query_create_event
event-listener.query.sls.queryCompletedName=aliyun sls log_store name, eg: query_completed_event
event-listener.query.sls.splitCompletedName=aliyun sls log_store name, eg: split_completed_event
```

#### Optional Parameters

* `event-listener.query.sls.queryCreatedName`   if empty, then the trino don't send query created event.
* `event-listener.query.sls.queryCompletedName` if empty, then the trino don't send query completed event
* `event-listener.query.sls.splitCompletedName` if empty, then the trino don't send split completed event. 
 
