package de.andre.tracing.aspect;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

public class TraceFileAnalyzer {

  private final ConfigHelper config;

  public TraceFileAnalyzer(ConfigHelper config) {
    this.config = config;
  }

  public static void main(String[] args) {
    ConfigHelper configHelper = new ConfigHelper(args);
    new TraceFileAnalyzer(configHelper)
        .analyze();
  }

  private void analyze() {
    String type = config.requiredValue("type");

    DataContainer dataContainer = new DataContainer()
        .parseFile(config);

    switch (type) {
      case "flat" -> new FlatAnalyzer().analyze(config, dataContainer);
      case "self" -> new SelfTimeAnalyzer().analyze(config, dataContainer);
      case "tree" -> new TreeTimeAnalyzer().analyze(config, dataContainer);
      default -> throw new IllegalArgumentException("Unknown -type " + type);
    }
  }

  private static class TreeTimeAnalyzer {

    private record SortKey(
        long startNs,
        int prio
    ) {

      public static SortKey header() {
        return new SortKey(0, 0);
      }
    }

    private enum OutputType {
      CSV,
      HTML
    }

    void analyze(ConfigHelper config, DataContainer dataContainer) {
      String filename = config.requiredValue("output");
      OutputType outputType = OutputType.CSV;
      if (filename.endsWith(".html")) {
        outputType = OutputType.HTML;
      }

      Map<MethodData, List<MethodWithSubCalls>> data = new HashMap<>();

      // 1st group by thread
      Map<ThreadData, List<Datapoint>> threadGroups = dataContainer.data()
          .stream()
          .collect(Collectors.groupingBy(Datapoint::thread));

      for (List<Datapoint> threadLocalCalls : threadGroups.values()) {
        var dataPerThread = collectThreadLocalData(threadLocalCalls);
        StatHelper.mergeInto(data, dataPerThread);
      }

      switch (outputType) {
        case CSV -> writeCsvFile(data, filename);
        case HTML -> writeHtmlFile(data, filename);
      }
    }

    private void writeHtmlFile(Map<MethodData, List<MethodWithSubCalls>> data, String filename) {
      SimpleFileFactory htmlFileFactory = new SimpleFileFactory();

      // write header
      htmlFileFactory.newLine().append("""
          <!DOCTYPE html>
          <html lang="en">
          <head>
            <title>Traces</title>
            <meta charset="UTF-8">
            <style>
              table, th, td {  border: 1px solid rgb(196, 196, 196);  border-collapse: collapse; font-family: OttoSans, Arial, Helvetica, sans-serif; font-size: 14px }
              th { font-weight: 700 }
              th, td {  padding: 5px;}
              td.num {  text-align: right;}
            </style>
          </head>
          <body>
          """);

      Map<MethodData, Long> firstMethodCall = new HashMap<>();
      for (Entry<MethodData, List<MethodWithSubCalls>> entry : data.entrySet()) {
        long firstStartNs = entry.getValue().stream()
            .mapToLong(MethodWithSubCalls::startNs)
            .min()
            .orElseThrow();
        firstMethodCall.put(entry.getKey(), firstStartNs);
      }
      List<Entry<MethodData, Long>> methodList = new ArrayList<>(firstMethodCall.entrySet());
      methodList.sort(Comparator
          .comparingLong((ToLongFunction<Entry<MethodData, Long>>) Entry::getValue)
          .thenComparing(e -> e.getKey().signatureWithoutReturnType(),
              String.CASE_INSENSITIVE_ORDER)
      );
      Map<MethodData, String> htmlAnchorNameLookUp = new HashMap<>();
      for (Entry<MethodData, Long> entry : methodList) {
        String anchorName =
            entry.getKey().extractMethodName() + "$" + (htmlAnchorNameLookUp.size() + 1);
        htmlAnchorNameLookUp.put(entry.getKey(), anchorName);
      }

      // write table-of-content
      htmlFileFactory.newLine().append("""
          <h1>All methods</h1>
          <ul>
          """);
      for (Entry<MethodData, Long> entry : methodList) {
        String anchorName = htmlAnchorNameLookUp.get(entry.getKey());
        htmlFileFactory.newLine().append("<li>")
            .append("<a href=\"#")
            .append(anchorName)
            .append("\">")
            .append(entry.getKey().shortSignature())
            .append("</a></li>");
      }
      htmlFileFactory.newLine().append("</ul>");

      // write section for each entry
      htmlFileFactory.newLine().append("<h1>All methods details</h1>");
      for (Entry<MethodData, Long> entry : methodList) {
        String anchorName = htmlAnchorNameLookUp.get(entry.getKey());
        htmlFileFactory.newLine()
            .append("<h3 id=\"").append(anchorName).append("\">Method: ")
            .append(entry.getKey().signatureWithoutReturnType())
            .append("</h3>");

        // merge all MethodWithSubCalls into one data
        List<MethodWithSubCalls> calls = data.get(entry.getKey());
        Map<MethodData, List<Long>> subCallDurations = new HashMap<>();
        Map<MethodData, Long> subCallStartNs = new HashMap<>();
        List<Long> callDurations = new ArrayList<>();
        List<Long> selfTimeNs = new ArrayList<>();

        Long minStartNs = null;
        for (MethodWithSubCalls call : calls) {
          callDurations.add(call.durationNs);
          selfTimeNs.add(call.selfTimeNs());
          if (minStartNs == null || call.startNs < minStartNs) {
            minStartNs = call.startNs;
          }

          for (MethodWithSubCalls.SubCallEntry subCall : call.sortedSubCalls()) {
            subCallDurations.computeIfAbsent(subCall.subMethod(), k -> new ArrayList<>())
                .add(subCall.durationNs());
            Long oldMinStartNs = subCallStartNs.get(subCall.subMethod());
            if (oldMinStartNs == null || subCall.startNs() < oldMinStartNs) {
              subCallStartNs.put(subCall.subMethod(), subCall.startNs());
            }
          }
        }
        assert minStartNs != null;

        // write entry for method call
        // write entry for method call
        callDurations.sort(Long::compareTo);
        long p50 = pDuration(callDurations, 50);
        long p90 = pDuration(callDurations, 90);
        long p95 = pDuration(callDurations, 95);
        long p99 = pDuration(callDurations, 99);
        long sum = callDurations.stream().mapToLong(Long::longValue).sum();
        int callCount = callDurations.size();

        htmlFileFactory.newLine().append("""
            <table>
              <tr>
                <th>method</th>
                <th class='num'>sum [ms]</th>
                <th class='num'>percent</th>
                <th class='num'>calls</th>
                <th class='num'>median</th>
                <th class='num'>p90 [ms]</th>
                <th class='num'>p95 [ms]</th>
                <th class='num'>p99 [ms]</th>
              </tr>
            """);
        htmlFileFactory.newLine().append("<tr>");
        htmlFileFactory.newLine()
            .append("<td>").append(entry.getKey().shortName()).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(sum)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>-</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toString(callCount)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p50)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p90)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p95)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p99)).append("</td>");
        htmlFileFactory.newLine().append("</tr>");

        // write entry for method call - self values
        selfTimeNs.sort(Long::compareTo);
        long p50self = pDuration(selfTimeNs, 50);
        long p90self = pDuration(selfTimeNs, 90);
        long p95self = pDuration(selfTimeNs, 95);
        long p99self = pDuration(selfTimeNs, 99);
        long sumSelf = selfTimeNs.stream().mapToLong(Long::longValue).sum();
        long percentSelf = sumSelf * 100 / sum;

        htmlFileFactory.newLine().append("<tr>");
        htmlFileFactory.newLine()
            .append("<td>.. &lt;&lt;self&gt;&gt;</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(sumSelf)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toString(percentSelf)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toString(callCount)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p50self)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p90self)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p95self)).append("</td>");
        htmlFileFactory.newLine()
            .append("<td class='num'>").append(toMs(p99self)).append("</td>");
        htmlFileFactory.newLine().append("</tr>");

        // TODO

        // write entries for sub calls
        ArrayList<Entry<MethodData, Long>> dataCopy = new ArrayList<>(subCallStartNs.entrySet());
        dataCopy.sort(Entry.comparingByValue());
        for (Entry<MethodData, Long> subCallEntry : dataCopy) {
          List<Long> subCallsDurationData = subCallDurations.get(subCallEntry.getKey());
          subCallsDurationData.sort(Long::compareTo);
          long p50sub = pDuration(subCallsDurationData, 50);
          long p90sub = pDuration(subCallsDurationData, 90);
          long p95sub = pDuration(subCallsDurationData, 95);
          long p99sub = pDuration(subCallsDurationData, 99);
          long sumSub = subCallsDurationData.stream().mapToLong(Long::longValue).sum();
          long percentSub = sumSub * 100 / sum;
          int callCountSub = subCallsDurationData.size();
          String subCallAnchorName = htmlAnchorNameLookUp.get(subCallEntry.getKey());

          htmlFileFactory.newLine().append("<tr>");
          htmlFileFactory.newLine()
              .append("<td>.. <a href=\"#").append(subCallAnchorName).append("\">")
              .append(subCallEntry.getKey().shortName()).append("</a></td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toMs(sumSub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toString(percentSub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toString(callCountSub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toMs(p50sub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toMs(p90sub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toMs(p95sub)).append("</td>");
          htmlFileFactory.newLine()
              .append("<td class='num'>").append(toMs(p99sub)).append("</td>");
          htmlFileFactory.newLine().append("</tr>");
        }

        htmlFileFactory.newLine().append("</table>");
      }

      // write footer
      htmlFileFactory.newLine().append("</body></html>");

      htmlFileFactory.writeToFile(filename);
    }

    private String toMs(long nanoSeconds) {
      long ms = StatHelper.nanosToMillis(nanoSeconds);
      return toString(ms);
    }

    private String toString(Number value) {
      return DecimalFormat.getNumberInstance(Locale.GERMANY).format(value);
    }

    private void writeCsvFile(Map<MethodData, List<MethodWithSubCalls>> data, String filename) {
      FileFactory<SortKey> csvFileFactory = new FileFactory<>(
          Comparator.comparing(SortKey::startNs)
              .thenComparing(SortKey::prio));

      // write header
      csvFileFactory.newLine(SortKey.header())
          .append("minStartNs;id;method;sumMs;percent;callCount;median;p90;p95;p99");

      int id = 1;
      for (Entry<MethodData, List<MethodWithSubCalls>> entry : data.entrySet()) {
        writeStatsAsCsv(entry.getKey(), id, entry.getValue(), csvFileFactory);
        id++;
      }

      csvFileFactory.writeToFile(filename);
    }

    private void writeStatsAsCsv(MethodData methodData, int id, List<MethodWithSubCalls> calls,
        FileFactory<SortKey> fw) {

      // merge all MethodWithSubCalls into one data
      Map<MethodData, List<Long>> subCallDurations = new HashMap<>();
      Map<MethodData, Long> subCallStartNs = new HashMap<>();
      List<Long> callDurations = new ArrayList<>();
      List<Long> selfTimeNs = new ArrayList<>();

      Long minStartNs = null;
      for (MethodWithSubCalls call : calls) {
        callDurations.add(call.durationNs);
        selfTimeNs.add(call.selfTimeNs());
        if (minStartNs == null || call.startNs < minStartNs) {
          minStartNs = call.startNs;
        }

        for (MethodWithSubCalls.SubCallEntry subCall : call.sortedSubCalls()) {
          subCallDurations.computeIfAbsent(subCall.subMethod(), k -> new ArrayList<>())
              .add(subCall.durationNs());
          Long oldMinStartNs = subCallStartNs.get(subCall.subMethod());
          if (oldMinStartNs == null || subCall.startNs() < oldMinStartNs) {
            subCallStartNs.put(subCall.subMethod(), subCall.startNs());
          }
        }
      }
      assert minStartNs != null;

      // write entry for method call
      callDurations.sort(Long::compareTo);
      long p50 = pDuration(callDurations, 50);
      long p90 = pDuration(callDurations, 90);
      long p95 = pDuration(callDurations, 95);
      long p99 = pDuration(callDurations, 99);
      long sum = callDurations.stream().mapToLong(Long::longValue).sum();
      fw.newLine(new SortKey(minStartNs, -100))
          .appendCsv(minStartNs)
          .appendCsv(id)
          .appendCsv(methodData.shortName())
          .appendCsv(StatHelper.nanosToMillis(sum))
          .appendCsv("100")
          .appendCsv(callDurations.size())
          .appendCsv(StatHelper.nanosToMillis(p50))
          .appendCsv(StatHelper.nanosToMillis(p90))
          .appendCsv(StatHelper.nanosToMillis(p95))
          .appendCsv(StatHelper.nanosToMillis(p99));

      // write entry for method call - self values
      selfTimeNs.sort(Long::compareTo);
      long p50self = pDuration(selfTimeNs, 50);
      long p90self = pDuration(selfTimeNs, 90);
      long p95self = pDuration(selfTimeNs, 95);
      long p99self = pDuration(selfTimeNs, 99);
      long sumSelf = selfTimeNs.stream().mapToLong(Long::longValue).sum();
      long percentSelf = sumSelf * 100 / sum;
      fw.newLine(new SortKey(minStartNs, -90))
          .appendCsv(minStartNs)
          .appendCsv(id)
          .appendCsv(".. <self>")
          .appendCsv(StatHelper.nanosToMillis(sumSelf))
          .appendCsv(percentSelf)
          .appendCsv(selfTimeNs.size())
          .appendCsv(StatHelper.nanosToMillis(p50self))
          .appendCsv(StatHelper.nanosToMillis(p90self))
          .appendCsv(StatHelper.nanosToMillis(p95self))
          .appendCsv(StatHelper.nanosToMillis(p99self));

      // write entries for sub calls
      ArrayList<Entry<MethodData, Long>> data = new ArrayList<>(subCallStartNs.entrySet());
      data.sort(Entry.comparingByValue());

      int subRow = 1;
      for (Entry<MethodData, Long> subCallEntry : data) {
        subRow++;
        List<Long> subCallsDurationData = subCallDurations.get(subCallEntry.getKey());
        subCallsDurationData.sort(Long::compareTo);
        long p50sub = pDuration(subCallsDurationData, 50);
        long p90sub = pDuration(subCallsDurationData, 90);
        long p95sub = pDuration(subCallsDurationData, 95);
        long p99sub = pDuration(subCallsDurationData, 99);
        long sumSub = subCallsDurationData.stream().mapToLong(Long::longValue).sum();
        long percentSub = sumSub * 100 / sum;
        int callCountSub = subCallsDurationData.size();
        fw.newLine(new SortKey(minStartNs, subRow))
            .appendCsv(minStartNs)
            .appendCsv(id)
            .appendCsv(".. ").append(subCallEntry.getKey().shortName())
            .appendCsv(StatHelper.nanosToMillis(sumSub))
            .appendCsv(percentSub)
            .appendCsv(callCountSub)
            .appendCsv(StatHelper.nanosToMillis(p50sub))
            .appendCsv(StatHelper.nanosToMillis(p90sub))
            .appendCsv(StatHelper.nanosToMillis(p95sub))
            .appendCsv(StatHelper.nanosToMillis(p99sub));
      }
    }

    private long pDuration(List<Long> sortedDurations, int pValue) {
      Long result = StatHelper.p(sortedDurations, pValue);
      if (result == null) {
        return 0;
      }
      return result;
    }

    private Map<MethodData, List<MethodWithSubCalls>> collectThreadLocalData(
        List<Datapoint> threadLocalCalls) {
      List<Datapoint> sortedCalls = new ArrayList<>(threadLocalCalls);
      sortedCalls.sort(Comparator.comparing(Datapoint::startNs));

      Map<MethodData, List<MethodWithSubCalls>> selfDurations = new HashMap<>();

      for (int i = 0; i < sortedCalls.size(); i++) {
        Datapoint callUnderInvestigation = sortedCalls.get(i);
        MethodWithSubCalls methodWithSubCalls = calculateMethodWithSubCalls(sortedCalls,
            callUnderInvestigation, i);

        selfDurations.computeIfAbsent(callUnderInvestigation.method(), k -> new ArrayList<>())
            .add(methodWithSubCalls);
      }

      return selfDurations;
    }

    private MethodWithSubCalls calculateMethodWithSubCalls(List<Datapoint> sortedCalls,
        Datapoint callUnderInvestigation, int startIndex) {
      List<MethodWithSubCalls.SubCallEntry> subCalls = new ArrayList<>();
      long ignoreCallsUntilNs = callUnderInvestigation.startNs();

      int index = startIndex + 1;
      long endNs = callUnderInvestigation.endNs();
      while (index < sortedCalls.size()) {
        Datapoint call = sortedCalls.get(index);
        index++;

        if (call.calledAfter(endNs)) {
          // exit loop
          break;
        }

        if (call.startNs() <= ignoreCallsUntilNs) {
          continue;
        }

        subCalls.add(
            new MethodWithSubCalls.SubCallEntry(call.method(), call.startNs(),
                call.durationNs()));
        ignoreCallsUntilNs = call.endNs();
      }

      return new MethodWithSubCalls(
          callUnderInvestigation.startNs(),
          callUnderInvestigation.durationNs(),
          subCalls);
    }

    private record MethodWithSubCalls(
        long startNs,
        long durationNs,
        List<SubCallEntry> subCalls
    ) {

      private MethodWithSubCalls(long startNs, long durationNs,
          List<SubCallEntry> subCalls) {
        this.startNs = startNs;
        this.durationNs = durationNs;
        this.subCalls = Collections.unmodifiableList(subCalls);
      }

      public long selfTimeNs() {
        return durationNs - subCalls.stream()
            .mapToLong(SubCallEntry::durationNs)
            .sum();
      }

      public List<SubCallEntry> sortedSubCalls() {
        Map<MethodData, List<SubCallEntry>> groups = subCalls.stream()
            .collect(Collectors.groupingBy(SubCallEntry::subMethod));

        Map<MethodData, SubCallEntry> subCallsSum = new HashMap<>();
        for (Entry<MethodData, List<SubCallEntry>> entry : groups.entrySet()) {
          MethodData key = entry.getKey();
          for (SubCallEntry subCallEntry : entry.getValue()) {
            SubCallEntry oldValue = subCallsSum.get(key);
            if (oldValue == null) {
              subCallsSum.put(key, subCallEntry);
            } else {
              SubCallEntry newValue = new SubCallEntry(oldValue.subMethod(),
                  Math.min(oldValue.startNs(), subCallEntry.startNs()),
                  oldValue.durationNs() + subCallEntry.durationNs());
              subCallsSum.put(key, newValue);
            }
          }
        }

        // sort by min start time
        List<SubCallEntry> result = new ArrayList<>(subCallsSum.values());
        result.sort(Comparator.comparing(SubCallEntry::startNs));
        return result;
      }

      private record SubCallEntry(
          MethodData subMethod,
          long startNs,
          long durationNs
      ) {

      }
    }
  }


  private static class SelfTimeAnalyzer {

    private record SortKey(
        long startNs,
        int row
    ) {

      public static SortKey header() {
        return new SortKey(0, 0);
      }
    }

    void analyze(ConfigHelper config, DataContainer dataContainer) {
      String filename = config.requiredValue("output");

      FileFactory<SortKey> fw = new FileFactory<>(
          Comparator.comparing(SortKey::startNs)
              .thenComparing(SortKey::row));

      // write header
      fw.newLine(SortKey.header())
          .append("signature;sumDuration;callCount;p50;p90;p95;p99");

      Map<MethodData, List<Long>> selfDurations = new HashMap<>();
      // 1st group by thread
      Map<ThreadData, List<Datapoint>> threadGroups = dataContainer.data()
          .stream()
          .collect(Collectors.groupingBy(Datapoint::thread));

      for (List<Datapoint> threadLocalCalls : threadGroups.values()) {
        Map<MethodData, List<Long>> selfDurationsOfThread
            = collectThreadLocalData(threadLocalCalls);
        StatHelper.mergeInto(selfDurations, selfDurationsOfThread);
      }

      int row = 100;
      for (Entry<MethodData, List<Long>> entry : selfDurations.entrySet()) {
        writeStats(entry.getKey(), entry.getValue(), fw, row);
        row++;
      }

      fw.writeToFile(filename);
    }

    private void writeStats(MethodData methodData, List<Long> selfDurations,
        FileFactory<SortKey> fw,
        int rowNumber) {
      int callCount = selfDurations.size();
      long sumDuration = selfDurations.stream()
          .mapToLong(Long::longValue)
          .sum();
      List<Long> sortedMethodCalls = new ArrayList<>(selfDurations);
      sortedMethodCalls.sort(Long::compare);

      long p50 = pDuration(sortedMethodCalls, 50);
      long p90 = pDuration(sortedMethodCalls, 90);
      long p95 = pDuration(sortedMethodCalls, 95);
      long p99 = pDuration(sortedMethodCalls, 99);

      fw.newLine(new SortKey(1, rowNumber))
          .appendCsv(methodData.shortName())
          .appendCsv(StatHelper.nanosToMillis(sumDuration))
          .appendCsv(callCount)
          .appendCsv(StatHelper.nanosToMillis(p50))
          .appendCsv(StatHelper.nanosToMillis(p90))
          .appendCsv(StatHelper.nanosToMillis(p95))
          .appendCsv(StatHelper.nanosToMillis(p99));
    }

    private long pDuration(List<Long> sortedDurations, int pValue) {
      var selfDuration = StatHelper.p(sortedDurations, pValue);
      if (selfDuration == null) {
        return 0;
      }
      return selfDuration;
    }

    private Map<MethodData, List<Long>> collectThreadLocalData(List<Datapoint> threadLocalCalls) {
      List<Datapoint> sortedCalls = new ArrayList<>(threadLocalCalls);
      sortedCalls.sort(Comparator.comparing(Datapoint::startNs));

      Map<MethodData, List<Long>> selfDurations = new HashMap<>();

      for (int i = 0; i < sortedCalls.size(); i++) {
        Datapoint callUnderInvestigation = sortedCalls.get(i);
        long selfTime = calculateSelfTime(sortedCalls, callUnderInvestigation, i);
        selfDurations.computeIfAbsent(callUnderInvestigation.method(), k -> new ArrayList<>())
            .add(selfTime);
      }

      return selfDurations;
    }

    private long calculateSelfTime(List<Datapoint> sortedCalls,
        Datapoint callUnderInvestigation, int startIndex) {
      long sumSubCalls = 0L;
      long ignoreCallsUntilNs = callUnderInvestigation.startNs();

      int index = startIndex + 1;
      long endNs = callUnderInvestigation.endNs();
      while (index < sortedCalls.size()) {
        Datapoint call = sortedCalls.get(index);
        index++;

        if (call.calledAfter(endNs)) {
          // exit loop
          break;
        }

        if (call.startNs() <= ignoreCallsUntilNs) {
          continue;
        }

        sumSubCalls += call.durationNs();
        ignoreCallsUntilNs = call.endNs();
      }

      return callUnderInvestigation.durationNs() - sumSubCalls;
    }
  }

  private static class FlatAnalyzer {

    private record SortKey(
        long startNs,
        int row
    ) {

      public static SortKey header() {
        return new SortKey(0, 0);
      }
    }

    void analyze(ConfigHelper config, DataContainer dataContainer) {
      String filename = config.requiredValue("output");

      FileFactory<SortKey> fw = new FileFactory<>(
          Comparator.comparing(SortKey::startNs)
              .thenComparing(SortKey::row));

      // write header
      fw.newLine(SortKey.header())
          .append("signature;sumDuration;callCount;p50;p90;p95;p99\n");

      Map<MethodData, List<Datapoint>> callData = dataContainer.data()
          .stream()
          .collect(Collectors.groupingBy(Datapoint::method));

      int row = 100;
      for (Entry<MethodData, List<Datapoint>> methodCallData : callData.entrySet()) {
        writeStats(methodCallData, fw, row);
        row++;
      }

      fw.writeToFile(filename);
    }

    private void writeStats(Entry<MethodData, List<Datapoint>> methodCallData,
        FileFactory<SortKey> fw,
        int rowNumber) {
      MethodData methodData = methodCallData.getKey();
      List<Datapoint> methodCalls = methodCallData.getValue();
      int callCount = methodCalls.size();
      long sumDuration = methodCalls.stream()
          .mapToLong(Datapoint::durationNs)
          .sum();
      long minStartNs = methodCalls.stream()
          .mapToLong(Datapoint::startNs)
          .min()
          .orElseThrow();
      List<Datapoint> sortedMethodCalls = new ArrayList<>(methodCalls);
      sortedMethodCalls.sort(Comparator.comparing(Datapoint::durationNs));

      long p50 = pDuration(sortedMethodCalls, 50);
      long p90 = pDuration(sortedMethodCalls, 90);
      long p95 = pDuration(sortedMethodCalls, 95);
      long p99 = pDuration(sortedMethodCalls, 99);

      fw.newLine(new SortKey(minStartNs, rowNumber))
          .appendCsv(methodData.shortName())
          .appendCsv(StatHelper.nanosToMillis(sumDuration))
          .appendCsv(callCount)
          .appendCsv(StatHelper.nanosToMillis(p50))
          .appendCsv(StatHelper.nanosToMillis(p90))
          .appendCsv(StatHelper.nanosToMillis(p95))
          .appendCsv(StatHelper.nanosToMillis(p99));
    }

    private long pDuration(List<Datapoint> sortedMethodCalls, int pValue) {
      var datapoint = StatHelper.p(sortedMethodCalls, pValue);
      if (datapoint == null) {
        return 0;
      }
      return datapoint.durationNs();
    }
  }

  private static class DataContainer {

    private final ThreadRepository threadRepository = new ThreadRepository();
    private final MethodRepository methodRepository = new MethodRepository();
    private final List<Datapoint> data = new ArrayList<>();

    public List<Datapoint> data() {
      return data;
    }

    public DataContainer parseFile(ConfigHelper config) {
      String filename = config.requiredValue("file");
      File file = new File(filename);
      try (LineNumberReader lnr = new LineNumberReader(
          new FileReader(file, StandardCharsets.UTF_8))) {

        String line;
        while ((line = lnr.readLine()) != null) {
          RawDatapoint datapoint = parseLine(line);

          ThreadData thread = threadRepository.find(datapoint.threadHashCode, datapoint.threadName);
          MethodData method = methodRepository.find(datapoint.signature);

          data.add(new Datapoint(datapoint.startNs, datapoint.durationNs, method, thread));
        }
      } catch (IOException ex) {
        ex.printStackTrace(System.err);
      }

      return this;
    }

    private RawDatapoint parseLine(String line) {
      String[] parts = line.split(";", 5);
      long startNs = Long.parseLong(parts[0]);
      long durationNs = Long.parseLong(parts[1]);
      String signature = parts[2];
      int threadHashCode = Integer.parseInt(parts[3]);
      String threadName = parts[4];

      return new RawDatapoint(startNs, durationNs, signature, threadHashCode, threadName);
    }
  }

  private static class ThreadRepository {

    private final Map<String, List<ThreadData>> data = new HashMap<>();

    public ThreadData find(int threadHashCode, String threadName) {
      List<ThreadData> list = data.get(threadName);
      if (list == null) {
        ArrayList<ThreadData> newList = new ArrayList<>();
        newList.add(new ThreadData(threadHashCode, threadName));
        data.put(threadName, newList);
        return newList.getFirst();
      }

      for (ThreadData threadData : list) {
        if (threadData.threadHashCode == threadHashCode
            && Objects.equals(threadData.threadName, threadName)) {
          return threadData;
        }
      }

      ThreadData threadData = new ThreadData(threadHashCode, threadName);
      list.add(threadData);
      return threadData;
    }
  }

  private record ThreadData(
      int threadHashCode,
      String threadName
  ) {

  }

  private static class MethodRepository {

    private final Map<String, MethodData> data = new HashMap<>();

    public MethodData find(String signature) {
      MethodData methodData = data.get(signature);
      if (methodData == null) {
        MethodData newMethodData = new MethodData(signature);
        data.put(signature, newMethodData);
        return newMethodData;
      }
      return methodData;
    }
  }

  private record MethodData(
      String signature
  ) {

    public String signatureWithoutReturnType() {
      int idx = signature.indexOf(' ');
      if (idx < 1) {
        return signature;
      }
      return signature.substring(idx + 1);
    }

    public String shortName() {
      String x = signatureWithoutReturnType();

      if (x.startsWith("de.otto.logistics.warehousecontingentcontrol.")) {
        x = x.substring("de.otto.logistics.warehousecontingentcontrol.".length());
      }

      int idx1 = x.indexOf('(');
      int idx2 = x.indexOf(')', idx1);
      if (idx2 > idx1 + 1) {
        x = x.substring(0, idx1 + 1) + ".." + x.substring(idx2);
      }

      return x;
    }

    public String extractMethodName() {
      int idx1 = signature.indexOf('(');
      if (idx1 < 0) {
        idx1 = signature.length();
      }
      String sub = signature.substring(0, idx1);
      int idx2 = sub.lastIndexOf('.');
      return sub.substring(idx2 + 1);
    }

    public String shortSignature() {
      String methodName = extractMethodName();
      int idx1 = signature.indexOf('(');
      if (idx1 < 0) {
        idx1 = signature.length();
      }
      String sub = signature.substring(0, idx1);
      int idx2 = sub.lastIndexOf('.');
      String fullClassName = sub.substring(0, idx2);

      int idx3 = fullClassName.lastIndexOf('.');
      String shortClassName = fullClassName.substring(idx3 + 1);

      return shortClassName + "." + methodName + "(..)";
    }
  }


  private record Datapoint(
      long startNs,
      long durationNs,
      MethodData method,
      ThreadData thread
  ) {

    private Datapoint {
      Objects.requireNonNull(method, "method");
      Objects.requireNonNull(thread, "thread");
    }

    public long endNs() {
      return startNs + durationNs;
    }

    public boolean calledAfter(long endNs) {
      return startNs >= endNs;
    }
  }

  private record RawDatapoint(
      long startNs,
      long durationNs,
      String signature,
      int threadHashCode,
      String threadName
  ) {

    private RawDatapoint {
      Objects.requireNonNull(signature, "signature");
      Objects.requireNonNull(threadName, "threadName");
      if (signature.isBlank()) {
        throw new IllegalArgumentException("signature must not be blank");
      }
      if (threadName.isBlank()) {
        throw new IllegalArgumentException("threadName must not be blank");
      }
    }
  }

  private static class FileFactory<T> {

    private final List<OutputLine<T>> lines = new ArrayList<>();
    private final Comparator<T> comparator;

    private FileFactory(Comparator<T> comparator) {
      this.comparator = comparator;
    }

    public OutputLine<T> newLine(T hiddenField) {
      var line = new OutputLine<>(hiddenField);
      lines.add(line);
      return line;
    }

    public void writeToFile(String fileName) {
      File file = new File(fileName);
      try (FileWriter fw = new FileWriter(file, StandardCharsets.UTF_8)) {
        lines.sort(Comparator.comparing(OutputLine::hiddenField, comparator));

        for (var line : lines) {
          fw.append(line.currentContent)
              .append("\n");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      System.out.println("File " + file.getAbsolutePath() + " written.");
    }

    protected static class OutputLine<T> {

      private final T hiddenField;
      private final StringBuilder currentContent = new StringBuilder();

      private OutputLine(T hiddenField) {
        this.hiddenField = hiddenField;
      }

      public T hiddenField() {
        return hiddenField;
      }

      public OutputLine<T> append(String value) {
        currentContent.append(value);
        return this;
      }

      public OutputLine<T> append(int value) {
        currentContent.append(value);
        return this;
      }

      public OutputLine<T> append(long value) {
        currentContent.append(value);
        return this;
      }

      private OutputLine<T> appendCsvDelimiterIfNeeded() {
        if (!currentContent.isEmpty()) {
          currentContent.append(";");
        }
        return this;
      }

      public OutputLine<T> appendCsv(String value) {
        return appendCsvDelimiterIfNeeded().append(value);
      }

      public OutputLine<T> appendCsv(int value) {
        return appendCsvDelimiterIfNeeded().append(value);

      }

      public OutputLine<T> appendCsv(long value) {
        return appendCsvDelimiterIfNeeded().append(value);
      }
    }
  }

  private static class SimpleFileFactory extends FileFactory<Integer> {

    private int currentLine = 0;

    public SimpleFileFactory() {
      super(Integer::compare);
    }

    public OutputLine<Integer> newLine() {
      return super.newLine(++currentLine);
    }
  }

  private interface StatHelper {

    long MILLIS_TO_NANOS = 1_000_000L;

    static <T> T p(List<T> sortedList, int pValue) {
      if (sortedList.isEmpty()) {
        return null;
      }

      int count = sortedList.size();
      int index = (pValue * count) / 100;
      if (index >= count) {
        index = count - 1;
      }
      return sortedList.get(index);
    }

    static <K, V> void mergeInto(Map<K, List<V>> destination, Map<K, List<V>> newData) {
      for (var newEntry : newData.entrySet()) {
        destination.computeIfAbsent(newEntry.getKey(), k -> new ArrayList<>())
            .addAll(newEntry.getValue());
      }
    }

    static long nanosToMillis(long nanoSeconds) {
      return nanoSeconds / MILLIS_TO_NANOS;
    }
  }
}
