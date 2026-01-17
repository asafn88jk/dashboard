package com.leumit.dashboard.run;

import com.leumit.dashboard.run.RunModel.Kind;
import com.leumit.dashboard.run.RunModel.LogEntry;
import com.leumit.dashboard.run.RunModel.RunNode;
import com.leumit.dashboard.run.SparkHtmlReportParser.Feature;
import com.leumit.dashboard.run.SparkHtmlReportParser.Log;
import com.leumit.dashboard.run.SparkHtmlReportParser.Scenario;
import com.leumit.dashboard.run.SparkHtmlReportParser.Step;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.leumit.dashboard.run.RunModel.*;

public final class RunModelParser {

    private RunModelParser() {}

    public static RunModel parseSparkHtml(Path reportHtml) throws Exception {
        if (!Files.exists(reportHtml)) {
            throw new IllegalArgumentException("Missing Spark HTML report: " + reportHtml);
        }

        List<Feature> features = SparkHtmlReportParser.parseFeatures(reportHtml);

        AtomicInteger seq = new AtomicInteger(1);
        Map<String, RunNode> byId = new LinkedHashMap<>();

        // Build a synthetic tree: ROOT -> GROUP -> ... -> FEATURE (leaf name) -> Scenario -> Step
        MutableNode root = new MutableNode("ROOT", Kind.ROOT, "ROOT", "ROOT", "", "", List.of());
        byId.put(root.id, root.freeze(byId));

        int featureCount = 0;

        for (Feature feature : features) {
            List<String> segments = feature.path() == null ? List.of() : feature.path();
            if (segments.isEmpty()) {
                segments = parseArrowPath(feature.name());
            }
            if (segments.isEmpty()) {
                segments = List.of(nonBlank(feature.name(), "Feature"));
            }

            List<RunNode> scenarioNodes = new ArrayList<>();
            for (Scenario sc : feature.scenarios()) {
                List<RunNode> stepNodes = new ArrayList<>();

                for (Step st : sc.steps()) {
                    List<LogEntry> logs = new ArrayList<>();
                    for (Log lg : st.logs()) {
                        logs.add(new LogEntry("", "", lg.detailsHtml(), lg.mediaPath()));
                    }

                    String stepId = makeId(Kind.STEP, st.text(), st.status(), seq.getAndIncrement());
                    RunNode stepNode = new RunNode(
                            stepId,
                            Kind.STEP,
                            st.text(),
                            st.text(),
                            st.status(),
                            "",
                            List.of(),
                            logs,
                            firstMedia(logs),
                            List.of()
                    );
                    stepNodes.add(stepNode);
                }

                String scId = makeId(Kind.SCENARIO, sc.name(), sc.status(), seq.getAndIncrement());
                RunNode scenarioNode = new RunNode(
                        scId,
                        Kind.SCENARIO,
                        sc.name(),
                        sc.name(),
                        sc.status(),
                        "",
                        List.of(),
                        List.of(),
                        null,
                        stepNodes
                );
                scenarioNodes.add(scenarioNode);
            }

            String featureLeafName = segments.get(segments.size() - 1);
            String featureId = makeId(Kind.FEATURE, feature.name(), feature.status(), seq.getAndIncrement());

            RunNode featureNode = new RunNode(
                    featureId,
                    Kind.FEATURE,
                    featureLeafName,
                    feature.name(),
                    feature.status(),
                    "",
                    segments,
                    List.of(),
                    null,
                    scenarioNodes
            );

            // Insert into breadcrumb group tree:
            MutableNode cursor = root;
            for (int i = 0; i < segments.size() - 1; i++) {
                cursor = cursor.childGroup(segments.get(i), seq);
            }
            cursor.addChild(featureNode);

            featureCount++;
        }

        // Freeze the mutable tree into immutable RunNodes and rebuild byId (fresh)
        Map<String, RunNode> frozenById = new LinkedHashMap<>();
        RunNode frozenRoot = root.freeze(frozenById);

        return new RunModel(frozenRoot, frozenById, featureCount);
    }

    private static String firstMedia(List<LogEntry> logs) {
        if (logs == null) return null;
        for (LogEntry lg : logs) {
            if (lg != null && lg.mediaPath() != null && !lg.mediaPath().isBlank()) {
                return lg.mediaPath();
            }
        }
        return null;
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
