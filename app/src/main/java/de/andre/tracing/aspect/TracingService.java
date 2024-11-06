package de.andre.tracing.aspect;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.stereotype.Component;

@Component
@EnableAspectJAutoProxy
public class TracingService {

  private static final Logger log = org.slf4j.LoggerFactory.getLogger(TracingService.class);

  private static final Queue<Datapoint> UNWRITTEN_TRACES = new ConcurrentLinkedQueue<>();

  public static void trace(
      long startNs,
      long durationNs,
      String signature,
      int threadHashCode,
      String threadName) {
    UNWRITTEN_TRACES.offer(
        new Datapoint(startNs, durationNs, signature, threadHashCode, threadName));
  }

  private final WritingThread writingThread;

  public TracingService() {
    String filename =
        "trace_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmm")) + ".trc";
    CsvTraceFileWriter csvTraceFileWriter = new CsvTraceFileWriter(filename);
    this.writingThread = new WritingThread(UNWRITTEN_TRACES, csvTraceFileWriter);
  }

  @PostConstruct
  public void init() {
    this.writingThread.start();
  }

  @PreDestroy
  public void tearDown() {
    this.writingThread.requestStop();
    try {
      this.writingThread.join(Duration.ofSeconds(20));
    } catch (InterruptedException e) {
      log.error("Failed to stop writingThread", e);
    }

    // force stop
    this.writingThread.interrupt();
  }

  private static class WritingThread extends Thread {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(TracingService.class);

    private final AtomicBoolean doStop = new AtomicBoolean(false);
    private final Queue<Datapoint> unwrittenTraces;
    private final TraceFileWriter traceFileWriter;

    private WritingThread(
        Queue<Datapoint> unwrittenTraces,
        TraceFileWriter traceFileWriter) {
      this.unwrittenTraces = unwrittenTraces;
      this.traceFileWriter = traceFileWriter;
    }

    public void requestStop() {
      this.doStop.set(true);
    }

    @Override
    public void run() {
      try {
        while (true) {
          Datapoint datapoint = unwrittenTraces.poll();
          if (datapoint == null) {
            if (doStop.get()) {
              log.info("All traces written and stop is requested.");
              break;
            }
            log.info("All traces written so far. Wait some time ...");
            Thread.sleep(2_000);
          } else {
            traceFileWriter.writeTraceToFile(datapoint);
          }
        }
      } catch (InterruptedException ex) {
        log.warn("Thread was interrupted", ex);
      } finally {
        traceFileWriter.close();
      }
    }
  }

  private record Datapoint(
      long startNs,
      long durationNs,
      String signature,
      int threadHashCode,
      String threadName
  ) {

  }

  private interface TraceFileWriter {

    void writeTraceToFile(Datapoint datapoint);

    void close();
  }

  private static class CsvTraceFileWriter implements TraceFileWriter {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(TracingService.class);

    private final String fileName;
    private FileWriter fileWriter = null;

    public CsvTraceFileWriter(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public void writeTraceToFile(Datapoint datapoint) {
      openFileWriterIfNeeded();

      StringBuilder logLine = new StringBuilder();
      logLine
          .append(datapoint.startNs)
          .append(";")
          .append(datapoint.durationNs)
          .append(";")
          .append(datapoint.signature)
          .append(";")
          .append(datapoint.threadHashCode)
          .append(";")
          .append(datapoint.threadName)
          .append("\n");
      try {
        fileWriter.append(logLine);
      } catch (IOException e) {
        log.error("Cannot write to file {}", new File(fileName).getAbsolutePath(), e);
        System.exit(1);
      }
    }

    private void openFileWriterIfNeeded() {
      if (fileWriter == null) {
        File file = new File(fileName);
        try {
          fileWriter = new FileWriter(file, StandardCharsets.UTF_8);
          log.info("Tracing file {} created.", file.getAbsolutePath());
        } catch (IOException e) {
          log.error("Cannot open file {}", file.getAbsolutePath(), e);
          System.exit(1);
        }
      }
    }

    @Override
    public void close() {
      if (fileWriter != null) {
        try {
          fileWriter.close();
          fileWriter = null;
        } catch (IOException e) {
          log.error("Cannot close file {}", fileName, e);
        }
      }
      log.info("Tracing file {} closed and written finished.", fileName);
    }
  }
}
