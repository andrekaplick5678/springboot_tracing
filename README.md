# Simple SpringBoot tracing

## Steps
1. start application ```app/src/main/java/de/andre/tracing/App.java```
2. make a request ```curl http://localhost:8080/doit``` - this will create a file like ```trace_202411060801.trc```
3. stop application

## Analyze the trace file

```text
run de.andre.tracing.aspect.TraceFileAnalyzer -type tree -file trace_202411060801.trc -output trace_tree.html
open trace_tree.html
```

```text
run de.andre.tracing.aspect.TraceFileAnalyzer -type tree -file trace_202411060801.trc -output trace_tree.csv
less trace_tree.csv
```
