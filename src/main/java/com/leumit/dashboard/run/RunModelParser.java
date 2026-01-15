package com.leumit.dashboard.run;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumit.dashboard.run.RunModel.Kind;
import com.leumit.dashboard.run.RunModel.LogEntry;
import com.leumit.dashboard.run.RunModel.RunNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.leumit.dashboard.run.RunModel.*;

public final class RunModelParser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private RunModelParser() {}

    public static RunModel parseExtentJson(Path extentJson) throws Exception {
        if (!Files.exists(extentJson)) {
            throw new IllegalArgumentException("Missing extent.json: " + extentJson);
        }

        JsonNode rootArr = MAPPER.readTree(Files.readString(extentJson));
        if (rootArr == null || !rootArr.isArray()) {
            throw new IllegalStateException("extent.json root is not an array (expected Feature[]).");
        }

        AtomicInteger seq = new AtomicInteger(1);
        Map<String, RunNode> byId = new LinkedHashMap<>();

        // Build a synthetic tree: ROOT -> GROUP -> ... -> FEATURE (leaf name) -> Scenario -> Step
        MutableNode root = new MutableNode("ROOT", Kind.ROOT, "ROOT", "ROOT", "", "", List.of());
        byId.put(root.id, root.freeze(byId));

        int featureCount = 0;

        for (JsonNode featureJson : rootArr) {
            if (featureJson == null || !featureJson.isObject()) continue;

            // parse the feature subtree (scenario/step/logs)
            RunNode parsedFeature = parseNodeRecursive(featureJson, seq, byId);

            // derive breadcrumb/group segments from "path" or from name split by "←"
            List<String> segments = readPathSegments(featureJson);
            if (segments.isEmpty()) {
                // fallback: treat as a single leaf
                segments = List.of(nonBlank(parsedFeature.getName(), "Feature"));
            }

            // all but last are GROUPS, last is FEATURE display
            String featureLeafName = segments.get(segments.size() - 1);
            RunNode featureWithLeafName = new RunNode(
                    parsedFeature.getId(),
                    Kind.FEATURE,
                    featureLeafName,
                    parsedFeature.getFullName(),
                    parsedFeature.getStatus(),
                    parsedFeature.getBddType(),
                    segments,
                    parsedFeature.getLogs(),
                    parsedFeature.getScreenshotPath(),
                    parsedFeature.getChildren()
            );

            // Insert into breadcrumb group tree:
            MutableNode cursor = root;
            for (int i = 0; i < segments.size() - 1; i++) {
                cursor = cursor.childGroup(segments.get(i), seq);
            }
            cursor.addChild(featureWithLeafName);

            featureCount++;
        }

        // Freeze the mutable tree into immutable RunNodes and rebuild byId (fresh)
        Map<String, RunNode> frozenById = new LinkedHashMap<>();
        RunNode frozenRoot = root.freeze(frozenById);

        return new RunModel(frozenRoot, frozenById, featureCount);
    }

    // -------------------- recursive parse --------------------

    private static RunNode parseNodeRecursive(JsonNode n, AtomicInteger seq, Map<String, RunNode> byId) {
        String bddType = n.path("bddType").asText("");
        Kind kind = kindFromBddType(bddType);

        String fullName = n.path("name").asText("");
        String name = n.path("displayName").asText("");
        if (name == null || name.isBlank()) name = fullName;

        String status = n.path("status").asText("");

        List<String> path = readPathSegments(n);

        // logs (usually on STEP/HOOK)
        List<LogEntry> logs = readLogs(n);

        String screenshot = logs.stream()
                .map(LogEntry::mediaPath)
                .filter(p -> p != null && !p.isBlank())
                .findFirst()
                .orElse(null);

        // children
        List<RunNode> children = new ArrayList<>();
        JsonNode kids = n.get("children");
        if (kids != null && kids.isArray()) {
            for (JsonNode ch : kids) {
                if (ch == null || !ch.isObject()) continue;
                children.add(parseNodeRecursive(ch, seq, byId));
            }
        }

        String id = makeId(kind, fullName, status, seq.getAndIncrement());
        RunNode out = new RunNode(id, kind, name, fullName, status, bddType, path, logs, screenshot, children);
        byId.put(id, out);
        return out;
    }

    private static Kind kindFromBddType(String bddType) {
        if (bddType == null) return Kind.NODE;
        if (bddType.endsWith(".Feature")) return Kind.FEATURE;
        if (bddType.endsWith(".Scenario")) return Kind.SCENARIO;

        if (bddType.endsWith(".Given") || bddType.endsWith(".When") || bddType.endsWith(".Then") || bddType.endsWith(".And"))
            return Kind.STEP;

        if (bddType.endsWith(".Asterisk")) return Kind.HOOK;
        return Kind.NODE;
    }

    // -------------------- breadcrumb segments --------------------

    private static List<String> readPathSegments(JsonNode n) {
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

        // derive from arrows in name
        String name = n.path("name").asText("").trim();
        if (name.contains("←")) {
            String[] parts = name.split("\\s*←\\s*");
            List<String> out = new ArrayList<>(parts.length);
            for (String part : parts) {
                String s = part.trim();
                if (!s.isBlank()) out.add(s);
            }
            return out;
        }

        return name.isBlank() ? List.of() : List.of(name);
    }

    // -------------------- logs --------------------

    private static List<LogEntry> readLogs(JsonNode n) {
        JsonNode logs = n.get("logs");
        if (logs == null || !logs.isArray()) return List.of();

        List<LogEntry> out = new ArrayList<>();
        for (JsonNode l : logs) {
            String ts = l.path("timestamp").asText("");
            String st = l.path("status").asText("");
            String details = l.path("details").asText("");

            String mediaPath = null;
            JsonNode media = l.get("media");
            if (media != null && media.isObject()) {
                String p = media.path("path").asText(null);
                if (p != null && !p.isBlank()) mediaPath = p;
            }

            out.add(new LogEntry(ts, st, details, mediaPath));
        }
        return out;
    }

    // -------------------- ids --------------------

    private static String makeId(Kind kind, String fullName, String status, int seq) {
        // deterministic-ish, readable, good enough for one run:
        int h = Objects.hash(kind, fullName, status);
        return kind.name() + ":" + Integer.toHexString(h) + ":" + seq;
    }

    private static String nonBlank(String s, String fallback) {
        return (s == null || s.isBlank()) ? fallback : s;
    }

    // -------------------- mutable breadcrumb tree --------------------

    private static final class MutableNode {
        final String id;
        final Kind kind;
        final String name;
        final String fullName;
        final String status;
        final String bddType;
        final List<String> path;

        final Map<String, MutableNode> groups = new LinkedHashMap<>();
        final List<RunNode> children = new ArrayList<>();

        MutableNode(String id, Kind kind, String name, String fullName, String status, String bddType, List<String> path) {
            this.id = id;
            this.kind = kind;
            this.name = name;
            this.fullName = fullName;
            this.status = status;
            this.bddType = bddType;
            this.path = path == null ? List.of() : List.copyOf(path);
        }

        MutableNode childGroup(String groupName, AtomicInteger seq) {
            return groups.computeIfAbsent(groupName, k -> new MutableNode(
                    makeId(Kind.GROUP, k, "", seq.getAndIncrement()),
                    Kind.GROUP,
                    k,
                    k,
                    "",
                    "",
                    List.of(k)
            ));
        }

        void addChild(RunNode n) { children.add(n); }

        RunNode freeze(Map<String, RunNode> outIndex) {
            // freeze groups first
            List<RunNode> frozenChildren = new ArrayList<>();

            for (MutableNode g : groups.values()) {
                frozenChildren.add(g.freeze(outIndex));
            }
            frozenChildren.addAll(children);

            RunNode frozen = new RunNode(id, kind, name, fullName, status, bddType, path, List.of(), null, frozenChildren);
            outIndex.put(frozen.getId(), frozen);

            // also index descendants that already have ids (features/scenarios/steps from parsed nodes)
            for (RunNode ch : frozenChildren) {
                indexAll(ch, outIndex);
            }
            return frozen;
        }

        private static void indexAll(RunNode n, Map<String, RunNode> outIndex) {
            outIndex.put(n.getId(), n);
            for (RunNode c : n.getChildren()) indexAll(c, outIndex);
        }
    }
}
