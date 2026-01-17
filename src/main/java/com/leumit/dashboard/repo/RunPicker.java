package com.leumit.dashboard.repo;

import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.run.ReportCutoff;
import com.leumit.dashboard.run.SparkHtmlReportParser;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class RunPicker {
  public Optional<PickedRun> pickLatest(Path baseDir, Pattern dirNamePattern) throws IOException {
    return pickLatestRuns(baseDir, dirNamePattern, 1).stream().findFirst();
  }

  public List<PickedRun> pickLatestRuns(Path baseDir, Pattern dirNamePattern, int limit) throws IOException {
    if (limit <= 0 || !Files.isDirectory(baseDir)) return List.of();

    try (Stream<Path> s = Files.list(baseDir)) {
      return s.filter(Files::isDirectory)
          .filter(p -> dirNamePattern.matcher(p.getFileName().toString()).matches())
          .map(this::toCandidate)
          .flatMap(Optional::stream)
          .sorted((a, b) -> b.lastModified().compareTo(a.lastModified()))
          .limit(limit)
          .map(c -> new PickedRun(c.runDir(), c.reportPath(), c.lastModified().toMillis(), c.summary()))
          .toList();
    }
  }

  private Optional<Candidate> toCandidate(Path runDir) {
    Optional<Path> report = SparkHtmlReportParser.findReportHtml(runDir);
    if (report.isEmpty()) return Optional.empty();
    Path reportPath = report.get();
    try {
      FileTime ts = Files.getLastModifiedTime(reportPath);
      ExtentSummary summary = SparkHtmlReportParser.parseSummary(reportPath);
      if (summary == null) return Optional.empty();
      if (SparkHtmlReportParser.isBeforeCutoff(summary, reportPath, ReportCutoff.CUTOFF_DATE)) {
        return Optional.empty();
      }
      return Optional.of(new Candidate(runDir, reportPath, ts, summary));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private record Candidate(Path runDir, Path reportPath, FileTime lastModified, ExtentSummary summary) {}

  public record PickedRun(Path runDir, Path reportPath, long lastModifiedMillis, ExtentSummary summary) {}
}
