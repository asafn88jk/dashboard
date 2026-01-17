package com.leumit.dashboard.repo;

import com.leumit.dashboard.model.ExtentSummary;
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
          .map(runDir -> toCandidate(runDir))
          .flatMap(Optional::stream)
          .sorted((a, b) -> b.lastModified().compareTo(a.lastModified()))
          .limit(limit)
          .map(c -> {
            try {
              ExtentSummary summary = SparkHtmlReportParser.parseSummary(c.reportPath());
              return new PickedRun(c.runDir(), c.reportPath(), c.lastModified().toMillis(), summary);
            } catch (IOException e) {
              return null;
            }
          })
          .filter(r -> r != null)
          .toList();
    }
  }

  private Optional<Candidate> toCandidate(Path runDir) {
    Optional<Path> report = SparkHtmlReportParser.findReportHtml(runDir);
    if (report.isEmpty()) return Optional.empty();
    try {
      FileTime ts = Files.getLastModifiedTime(report.get());
      return Optional.of(new Candidate(runDir, report.get(), ts));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private record Candidate(Path runDir, Path reportPath, FileTime lastModified) {}

  public record PickedRun(Path runDir, Path reportPath, long lastModifiedMillis, ExtentSummary summary) {}
}
