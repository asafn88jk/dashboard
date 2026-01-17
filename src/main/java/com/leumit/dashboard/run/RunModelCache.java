package com.leumit.dashboard.run;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.nio.file.*;
import java.nio.file.attribute.FileTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class RunModelCache {

    private record Cached(FileTime lastModified, RunModel model) {}

    private final Map<Path, Cached> cache = new ConcurrentHashMap<>();

    public RunModel getOrLoad(Path runDir) throws Exception {
        Path dir = runDir.toAbsolutePath().normalize();
        Path reportHtml = SparkHtmlReportParser.requireReportHtml(dir);

        FileTime lm = Files.getLastModifiedTime(reportHtml);

        Cached existing = cache.get(dir);
        if (existing != null && existing.lastModified.equals(lm)) {
            return existing.model;
        }

        RunModel parsed = RunModelParser.parseSparkHtml(reportHtml);
        cache.put(dir, new Cached(lm, parsed));
        log.info("Parsed Spark HTML into RunModel: dir={}, features={}", dir, parsed.getFeatureCount());
        return parsed;
    }

    public void invalidate(Path runDir) {
        if (runDir == null) return;
        cache.remove(runDir.toAbsolutePath().normalize());
    }
}
