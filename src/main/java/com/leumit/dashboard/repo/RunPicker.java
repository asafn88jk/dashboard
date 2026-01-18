package com.leumit.dashboard.repo;

import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.run.ReportCache;
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
  private final ReportCache reportCache;
  private final RunPickerCache runPickerCache;

  public RunPicker(ReportCache reportCache, RunPickerCache runPickerCache) {
    this.reportCache = reportCache;
    this.runPickerCache = runPickerCache;
  }

  public Optional<PickedRun> pickLatest(Path baseDir, Pattern dirNamePattern) throws IOException {
    return pickLatestFast(baseDir, dirNamePattern);
  }

  public List<PickedRun> pickLatestRuns(Path baseDir, Pattern dirNamePattern, int limit) throws IOException {
    if (limit <= 0 || !Files.isDirectory(baseDir)) return List.of();

    long startNs = System.nanoTime();
    CandidateFetch fetch = fetchCandidates(baseDir, dirNamePattern);
    List<Candidate> sorted = new ArrayList<>(fetch.candidates());
    sorted.sort((a, b) -> b.lastModified().compareTo(a.lastModified()));

    long summaryStartNs = System.nanoTime();
    List<PickedRun> out = new ArrayList<>();
    for (Candidate c : sorted) {
      if (out.size() >= limit) break;
      try {
        ExtentSummary summary = reportCache.getSummary(c.reportPath());
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
    log.info("RunPicker.pickLatestRuns baseDir={} limit={} matchedDirs={} candidates={} selected={} cache={} scanMs={} summaryMs={} totalMs={}",
            baseDir, limit, fetch.matchedDirs(), fetch.candidates().size(), out.size(),
            fetch.cacheHit() ? "hit" : "miss", fetch.scanMs(), summaryMs, totalMs);
    return out;
  }

  public Optional<PickedRun> pickLatestFast(Path baseDir, Pattern dirNamePattern) throws IOException {
    if (!Files.isDirectory(baseDir)) return Optional.empty();

    long startNs = System.nanoTime();
    CandidateFetch fetch = fetchCandidates(baseDir, dirNamePattern);
    Candidate best = bestCandidate(fetch.candidates());

    if (best == null) {
      long totalMs = (System.nanoTime() - startNs) / 1_000_000;
      log.info("RunPicker.pickLatestFast baseDir={} matchedDirs={} candidates=0 cache={} scanMs={} totalMs={}",
              baseDir, fetch.matchedDirs(), fetch.cacheHit() ? "hit" : "miss", fetch.scanMs(), totalMs);
      return Optional.empty();
    }

    long summaryStartNs = System.nanoTime();
    try {
      ExtentSummary summary = reportCache.getSummary(best.reportPath());
      if (summary == null) return Optional.empty();
      if (SparkHtmlReportParser.isBeforeCutoff(summary, best.reportPath(), ReportCutoff.CUTOFF_DATE)) {
        return Optional.empty();
      }
      long summaryMs = (System.nanoTime() - summaryStartNs) / 1_000_000;
      long totalMs = (System.nanoTime() - startNs) / 1_000_000;
      log.info("RunPicker.pickLatestFast baseDir={} matchedDirs={} candidates={} cache={} scanMs={} summaryMs={} totalMs={}",
              baseDir, fetch.matchedDirs(), fetch.candidates().size(), fetch.cacheHit() ? "hit" : "miss",
              fetch.scanMs(), summaryMs, totalMs);
      return Optional.of(new PickedRun(best.runDir(), best.reportPath(), best.lastModified().toMillis(), summary));
    } catch (IOException ignored) {
      return Optional.empty();
    }
  }

  private CandidateFetch fetchCandidates(Path baseDir, Pattern dirNamePattern) throws IOException {
    Optional<RunPickerCache.CachedCandidates> cached = runPickerCache.get(baseDir, dirNamePattern);
    if (cached.isPresent()) {
      RunPickerCache.CachedCandidates hit = cached.get();
      List<Candidate> candidates = candidatesFromCache(hit);
      return new CandidateFetch(candidates, hit.matchedDirs(), true, 0);
    }

    long scanStartNs = System.nanoTime();
    int[] matchedDirs = new int[1];
    List<Candidate> candidates = new ArrayList<>();

    try (Stream<Path> s = Files.list(baseDir)) {
      s.filter(Files::isDirectory)
          .filter(p -> {
            boolean matches = dirNamePattern.matcher(p.getFileName().toString()).matches();
            if (matches) matchedDirs[0]++;
            return matches;
          })
          .map(this::toCandidate)
          .flatMap(Optional::stream)
          .forEach(candidates::add);
    }

    long scanMs = (System.nanoTime() - scanStartNs) / 1_000_000;
    runPickerCache.put(baseDir, dirNamePattern,
            new RunPickerCache.CachedCandidates(System.currentTimeMillis(), matchedDirs[0], toEntries(candidates)));
    return new CandidateFetch(candidates, matchedDirs[0], false, scanMs);
  }

  private static Candidate bestCandidate(List<Candidate> candidates) {
    Candidate best = null;
    for (Candidate c : candidates) {
      if (best == null || c.lastModified().compareTo(best.lastModified()) > 0) {
        best = c;
      }
    }
    return best;
  }

  private static List<RunPickerCache.CandidateEntry> toEntries(List<Candidate> candidates) {
    List<RunPickerCache.CandidateEntry> out = new ArrayList<>();
    for (Candidate c : candidates) {
      out.add(new RunPickerCache.CandidateEntry(
              c.runDir().toString(),
              c.reportPath().toString(),
              c.lastModified().toMillis()
      ));
    }
    return out;
  }

  private static List<Candidate> candidatesFromCache(RunPickerCache.CachedCandidates cached) {
    List<Candidate> out = new ArrayList<>();
    for (RunPickerCache.CandidateEntry entry : cached.candidates()) {
      Path runDir = Path.of(entry.runDir());
      Path reportPath = Path.of(entry.reportPath());
      if (!Files.exists(reportPath)) continue;
      out.add(new Candidate(runDir, reportPath, FileTime.fromMillis(entry.lastModifiedMillis())));
    }
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

  private record CandidateFetch(List<Candidate> candidates, int matchedDirs, boolean cacheHit, long scanMs) {}

  public record PickedRun(Path runDir, Path reportPath, long lastModifiedMillis, ExtentSummary summary) {}
}
