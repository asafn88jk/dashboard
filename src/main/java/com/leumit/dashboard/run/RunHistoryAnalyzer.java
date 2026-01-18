package com.leumit.dashboard.run;

import com.leumit.dashboard.run.SparkHtmlReportParser.Feature;
import com.leumit.dashboard.run.SparkHtmlReportParser.Scenario;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class RunHistoryAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(RunHistoryAnalyzer.class);
    private static final String PATH_SEP = " / ";
    private static final String KEY_SEP = " :: ";
    private static final Map<Path, CachedStatuses> STATUS_CACHE = new ConcurrentHashMap<>();

    private RunHistoryAnalyzer() {}

    private record CachedStatuses(FileTime lastModified, Map<String, String> statuses) {}

    public record FlakySummary(int flakyScenarios, int totalScenarios) {}

    public static FlakySummary computeFlakySummary(List<Path> runDirs) {
        if (runDirs == null || runDirs.isEmpty()) {
            return new FlakySummary(0, 0);
        }

        Map<String, List<String>> history = new HashMap<>();
        for (Path runDir : runDirs) {
            if (runDir == null) continue;
            try {
                Path reportHtml = SparkHtmlReportParser.requireReportHtml(runDir);
                Map<String, String> statuses = parseScenarioStatuses(reportHtml);
                for (Map.Entry<String, String> entry : statuses.entrySet()) {
                    history.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .add(entry.getValue());
                }
            } catch (Exception ignored) {
                // Ignore bad runs for flakiness metrics
            }
        }

        int flaky = 0;
        for (List<String> statuses : history.values()) {
            if (isFlaky(statuses)) {
                flaky++;
            }
        }
        return new FlakySummary(flaky, history.size());
    }

    public static Map<String, String> parseScenarioStatuses(Path reportHtml) throws Exception {
        if (!Files.exists(reportHtml)) {
            throw new IllegalArgumentException("Missing Spark HTML report: " + reportHtml);
        }

        Path cacheKey = reportHtml.toAbsolutePath().normalize();
        FileTime lm = Files.getLastModifiedTime(cacheKey);
        CachedStatuses cached = STATUS_CACHE.get(cacheKey);
        if (cached != null && cached.lastModified.equals(lm)) {
            log.info("RunHistoryAnalyzer cache hit report={} scenarios={}",
                    cacheKey.getFileName(), cached.statuses.size());
            return cached.statuses;
        }

        long startNs = System.nanoTime();
        Map<String, String> out = new HashMap<>();

        List<Feature> features = SparkHtmlReportParser.parseFeatures(reportHtml);
        for (Feature f : features) {
            List<String> fullPath = f.path() == null ? List.of() : f.path();
            if (fullPath.isEmpty()) {
                fullPath = parseArrowPath(f.name());
            }

            String featureTitle = !fullPath.isEmpty()
                    ? fullPath.get(fullPath.size() - 1)
                    : (f.name() == null ? "" : f.name().trim());

            List<String> normalizedPath;
            if (fullPath.isEmpty()) {
                normalizedPath = featureTitle.isBlank() ? List.of() : List.of(featureTitle);
            } else {
                normalizedPath = new ArrayList<>(fullPath);
                normalizedPath.set(normalizedPath.size() - 1, featureTitle);
            }

            Map<String, Integer> nameCounts = new HashMap<>();
            for (Scenario s : f.scenarios()) {
                String sName = clean(s.name());
                nameCounts.merge(sName, 1, Integer::sum);
            }

            Map<String, Integer> nameSeen = new HashMap<>();
            for (Scenario s : f.scenarios()) {
                String sName = clean(s.name());
                int ordinal = nameSeen.merge(sName, 1, Integer::sum);
                boolean duplicate = nameCounts.getOrDefault(sName, 0) > 1;
                String keyName = duplicate ? (sName + " #" + ordinal) : sName;
                String sStatus = s.status();
                String scenarioKey = scenarioKey(normalizedPath, keyName);
                if (!scenarioKey.isBlank()) {
                    out.put(scenarioKey, sStatus);
                }
            }
        }

        Map<String, String> frozen = Collections.unmodifiableMap(out);
        STATUS_CACHE.put(cacheKey, new CachedStatuses(lm, frozen));
        long totalMs = (System.nanoTime() - startNs) / 1_000_000;
        log.info("RunHistoryAnalyzer parsed report={} scenarios={} totalMs={}",
                cacheKey.getFileName(), frozen.size(), totalMs);
        return frozen;
    }

    public static boolean isFlaky(Collection<String> statuses) {
        if (statuses == null || statuses.isEmpty()) return false;

        boolean sawPass = false;
        boolean sawFail = false;

        for (String status : statuses) {
            String norm = normalizeStatus(status);
            if ("PASS".equals(norm)) {
                sawPass = true;
            } else if ("FAIL".equals(norm) || "KNOWNBUG".equals(norm)) {
                sawFail = true;
            }
        }

        return sawPass && sawFail;
    }

    public static String normalizeStatus(String status) {
        if (status == null) return "UNKNOWN";
        return switch (status.toUpperCase(Locale.ROOT)) {
            case "PASS" -> "PASS";
            case "FAIL" -> "FAIL";
            case "SKIP" -> "SKIP";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "KNOWNBUG";
            case "INFO" -> "INFO";
            default -> "UNKNOWN";
        };
    }

    public static String scenarioKey(List<String> fullPath, String scenarioName) {
        String featureKey = featurePathKey(fullPath);
        String scenario = clean(scenarioName);
        if (featureKey.isBlank() && scenario.isBlank()) return "";
        if (featureKey.isBlank()) return scenario;
        if (scenario.isBlank()) return featureKey;
        return featureKey + KEY_SEP + scenario;
    }

    public static String featurePathKey(List<String> fullPath) {
        if (fullPath == null || fullPath.isEmpty()) return "";
        List<String> cleaned = new ArrayList<>();
        for (String part : fullPath) {
            String t = clean(part);
            if (!t.isBlank()) cleaned.add(t);
        }
        return String.join(PATH_SEP, cleaned);
    }

    private static List<String> parseArrowPath(String s) {
        if (s == null) return List.of();
        if (!s.contains("\u2190")) {
            String t = s.trim();
            return t.isBlank() ? List.of() : List.of(t);
        }

        String[] parts = s.split("\\s*\\u2190\\s*");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isBlank()) out.add(t);
        }
        return out;
    }

    private static String clean(String s) {
        return s == null ? "" : s.trim();
    }
}
