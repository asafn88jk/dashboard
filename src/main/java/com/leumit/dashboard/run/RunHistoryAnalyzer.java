package com.leumit.dashboard.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public final class RunHistoryAnalyzer {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PATH_SEP = " / ";
    private static final String KEY_SEP = " :: ";

    private RunHistoryAnalyzer() {}

    public record FlakySummary(int flakyScenarios, int totalScenarios) {}

    public static FlakySummary computeFlakySummary(List<Path> runDirs) {
        if (runDirs == null || runDirs.isEmpty()) {
            return new FlakySummary(0, 0);
        }

        Map<String, List<String>> history = new HashMap<>();
        for (Path runDir : runDirs) {
            if (runDir == null) continue;
            Path extent = runDir.resolve("extent.json");
            try {
                Map<String, String> statuses = parseScenarioStatuses(extent);
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

    public static Map<String, String> parseScenarioStatuses(Path extentJson) throws Exception {
        if (!Files.exists(extentJson)) {
            throw new IllegalArgumentException("Missing extent.json: " + extentJson);
        }

        JsonNode rootArr = MAPPER.readTree(Files.readString(extentJson));
        if (rootArr == null || !rootArr.isArray()) {
            throw new IllegalStateException("extent.json root is not an array (expected Feature[]).");
        }

        Map<String, String> out = new HashMap<>();

        for (JsonNode f : rootArr) {
            if (!f.isObject()) continue;
            String bddType = f.path("bddType").asText("");
            if (!bddType.endsWith(".Feature")) continue;

            String featureName = f.path("name").asText("").trim();
            String featureDisplay = f.path("displayName").asText("").trim();
            List<String> fullPath = readFeaturePath(f, featureName);

            String featureTitle = !featureDisplay.isBlank()
                    ? featureDisplay
                    : (!fullPath.isEmpty() ? fullPath.get(fullPath.size() - 1) : featureName);

            List<String> normalizedPath;
            if (fullPath.isEmpty()) {
                normalizedPath = featureTitle.isBlank() ? List.of() : List.of(featureTitle);
            } else {
                normalizedPath = new ArrayList<>(fullPath);
                normalizedPath.set(normalizedPath.size() - 1, featureTitle);
            }

            JsonNode scenArr = f.get("children");
            if (scenArr == null || !scenArr.isArray()) continue;

            for (JsonNode s : scenArr) {
                if (!s.isObject()) continue;
                String sType = s.path("bddType").asText("");
                if (!sType.endsWith(".Scenario")) continue;

                String sName = pickName(s);
                String sStatus = s.path("status").asText("");
                String key = scenarioKey(normalizedPath, sName);
                if (!key.isBlank()) {
                    out.put(key, sStatus);
                }
            }
        }

        return out;
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

    private static String pickName(JsonNode n) {
        String dn = n.path("displayName").asText("");
        if (dn != null && !dn.isBlank()) return dn.trim();
        return n.path("name").asText("").trim();
    }

    private static List<String> parseArrowPath(String s) {
        if (s == null) return List.of();
        if (!s.contains("←")) {
            String t = s.trim();
            return t.isBlank() ? List.of() : List.of(t);
        }

        String[] parts = s.split("\\s*←\\s*");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isBlank()) out.add(t);
        }
        return out;
    }

    private static List<String> readFeaturePath(JsonNode n, String fallbackName) {
        JsonNode p = n.get("path");
        if (p != null && p.isArray()) {
            List<String> out = new ArrayList<>();
            for (JsonNode x : p) {
                if (x != null && x.isTextual()) {
                    String s = x.asText("").trim();
                    if (!s.isBlank()) out.add(s);
                }
            }
            if (!out.isEmpty()) return out;
        }
        return parseArrowPath(fallbackName);
    }

    private static String clean(String s) {
        return s == null ? "" : s.trim();
    }
}
