package com.leumit.dashboard.repo;

import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.run.ReportCutoff;
import com.leumit.dashboard.run.SparkHtmlReportParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
@Slf4j
public class RunPicker {
  public Optional<PickedRun> pickLatest(Path baseDir, Pattern dirNamePattern) throws IOException {
    return pickLatestRuns(baseDir, dirNamePattern, 1).stream().findFirst();
  }

  public List<PickedRun> pickLatestRuns(Path baseDir, Pattern dirNamePattern, int limit) throws IOException {
    if (limit <= 0 || !Files.isDirectory(baseDir)) return List.of();

    long startNs = System.nanoTime();
    int buffer = Math.max(limit * 3, limit + 10);
    PriorityQueue<Candidate> top = new PriorityQueue<>(Comparator.comparing(Candidate::lastModified));
    int[] matchedDirs = new int[1];
    int[] candidates = new int[1];

    long scanStartNs = System.nanoTime();
    try (Stream<Path> s = Files.list(baseDir)) {
      s.filter(Files::isDirectory)
          .filter(p -> {
            boolean matches = dirNamePattern.matcher(p.getFileName().toString()).matches();
            if (matches) matchedDirs[0]++;
            return matches;
          })
          .map(this::toCandidate)
          .flatMap(Optional::stream)
          .peek(c -> candidates[0]++)
          .forEach(c -> pushCandidate(top, c, buffer));
    }
    long scanMs = (System.nanoTime() - scanStartNs) / 1_000_000;

    List<Candidate> sorted = new ArrayList<>(top);
    sorted.sort((a, b) -> b.lastModified().compareTo(a.lastModified()));

    long summaryStartNs = System.nanoTime();
    List<PickedRun> out = new ArrayList<>();
    for (Candidate c : sorted) {
      if (out.size() >= limit) break;
      try {
        ExtentSummary summary = SparkHtmlReportParser.parseSummary(c.reportPath());
        if (summary == null) continue;
        if (SparkHtmlReportParser.isBeforeCutoff(summary, c.reportPath(), ReportCutoff.CUTOFF_DATE)) {
          continue;
        }
        out.add(new PickedRun(c.runDir(), c.reportPath(), c.lastModified().toMillis(), summary));
      } catch (IOException ignored) {
        // skip bad report
      }
    }
    long summaryMs = (System.nanoTime() - summaryStartNs) / 1_000_000;
    long totalMs = (System.nanoTime() - startNs) / 1_000_000;
    log.info("RunPicker.pickLatestRuns baseDir={} limit={} matchedDirs={} candidates={} selected={} scanMs={} summaryMs={} totalMs={}",
            baseDir, limit, matchedDirs[0], candidates[0], out.size(), scanMs, summaryMs, totalMs);
    return out;
  }

  private Optional<Candidate> toCandidate(Path runDir) {
    Optional<Path> report = SparkHtmlReportParser.findReportHtml(runDir);
    if (report.isEmpty()) return Optional.empty();
    Path reportPath = report.get();
    try {
      FileTime ts = Files.getLastModifiedTime(reportPath);
      if (isBeforeCutoff(ts, ReportCutoff.CUTOFF_DATE)) {
        return Optional.empty();
      }
      return Optional.of(new Candidate(runDir, reportPath, ts));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private static void pushCandidate(PriorityQueue<Candidate> top, Candidate c, int limit) {
    if (top.size() < limit) {
      top.add(c);
      return;
    }
    Candidate oldest = top.peek();
    if (oldest != null && c.lastModified().compareTo(oldest.lastModified()) > 0) {
      top.poll();
      top.add(c);
    }
  }

  private static boolean isBeforeCutoff(FileTime ts, LocalDate cutoff) {
    if (ts == null || cutoff == null) return false;
    LocalDate date = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.toMillis()), ZoneId.systemDefault())
            .toLocalDate();
    return date.isBefore(cutoff);
  }

  private record Candidate(Path runDir, Path reportPath, FileTime lastModified) {}

  public record PickedRun(Path runDir, Path reportPath, long lastModifiedMillis, ExtentSummary summary) {}
}
