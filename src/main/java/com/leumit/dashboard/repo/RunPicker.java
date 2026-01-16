package com.leumit.dashboard.repo;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumit.dashboard.model.ExtentSummary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class RunPicker {

  private final ObjectMapper mapper = new ObjectMapper();

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
              ExtentSummary summary = mapper.readValue(Files.readString(c.summaryPath()), ExtentSummary.class);
              return new PickedRun(c.runDir(), c.summaryPath(), c.lastModified().toMillis(), summary);
            } catch (IOException e) {
              return null;
            }
          })
          .filter(r -> r != null)
          .toList();
    }
  }

  private Optional<Candidate> toCandidate(Path runDir) {
    Path summary = runDir.resolve("extent.summary.json");
    if (!Files.isRegularFile(summary)) return Optional.empty();
    try {
      FileTime ts = Files.getLastModifiedTime(summary);
      return Optional.of(new Candidate(runDir, summary, ts));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private record Candidate(Path runDir, Path summaryPath, FileTime lastModified) {}

  public record PickedRun(Path runDir, Path summaryPath, long lastModifiedMillis, ExtentSummary summary) {}
}
