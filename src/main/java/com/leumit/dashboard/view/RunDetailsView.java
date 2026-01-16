package com.leumit.dashboard.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumit.dashboard.config.DashboardFiltersProperties;
import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.repo.RunPicker;
import com.leumit.dashboard.run.RunHistoryAnalyzer;
import lombok.extern.slf4j.Slf4j;
import org.primefaces.event.NodeSelectEvent;
import org.primefaces.model.DefaultTreeNode;
import org.primefaces.model.TreeNode;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component("runDetailsView")
@Scope("view")
public class RunDetailsView implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Extent time strings look like: "Jan 14, 2026, 9:28:24 AM" (note narrow no-break space)
    private static final DateTimeFormatter EXTENT_TIME_FMT =
            DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm:ss a", Locale.ENGLISH);
    private static final DateTimeFormatter RUN_LABEL_FMT =
            DateTimeFormatter.ofPattern("dd.MM HH:mm", Locale.ENGLISH);
    private static final Pattern LOG_TS_PATTERN = Pattern.compile(
            "<p[^>]*class=['\"]timestamp['\"][^>]*>([^<]+)</p>",
            Pattern.CASE_INSENSITIVE
    );
    private static final Pattern LOG_LOCALTIME_PATTERN = Pattern.compile(
            "<p[^>]*class=['\"]localtime['\"][^>]*>.*?</p>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final int HISTORY_LIMIT = 7;

    private final DashboardFiltersProperties props;
    private final RunPicker runPicker;

    // view params
    private String filter;
    private String item;
    private String run;

    private boolean loaded;
    private String loadedKey;
    private String error;

    private Path runDir;
    private ExtentSummary summary;

    // domain model
    private List<FeatureModel> features = List.of();
    private FeatureModel selectedFeature;

    // features tree (GROUP -> FEATURE leaves)
    private TreeNode<TreeItem> featureTreeRoot;
    private TreeNode<TreeItem> selectedTreeNode;
    private final Map<String, TreeNode<TreeItem>> featureIdToTreeNode = new HashMap<>();

    // history/flaky
    private List<RunOption> recentRuns = List.of();
    private final Map<String, String> runLabelsByFolder = new HashMap<>();
    private final Map<String, ScenarioHistory> scenarioHistoryByKey = new HashMap<>();
    private final Map<String, Integer> featureFlakyCounts = new HashMap<>();
    private int flakyScenarioCount;

    public RunDetailsView(DashboardFiltersProperties props, RunPicker runPicker) {
        this.props = props;
        this.runPicker = runPicker;
        // IMPORTANT: use typed root so <p:treeNode type="ROOT"> can match if you define it (optional)
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0, 0),
                null
        );
    }

    /**
     * Called from <f:event type="preRenderView" .../>
     * Runs AFTER <f:viewParam> is applied.
     */
    public void preRender() {
        String key = String.join("|", String.valueOf(filter), String.valueOf(item), String.valueOf(run));
        if (loaded && Objects.equals(loadedKey, key)) return;
        loaded = true;
        loadedKey = key;

        try {
            this.error = null;
            if (isBlank(filter) || isBlank(item) || isBlank(run)) {
                throw new IllegalArgumentException("Missing URL params: filter/item/run");
            }

            DashboardFiltersProperties.Item itemConfig = resolveItemConfig(filter, item);
            this.runDir = resolveRunDir(itemConfig, run);

            Path summaryPath = runDir.resolve("extent.summary.json");
            this.summary = readSummary(summaryPath);

            Path extentPath = runDir.resolve("extent.json");
            this.features = parseExtentToModel(extentPath);

            loadRunHistory(itemConfig, summaryPath);
            computeFeatureFlakyCounts();

            buildFeatureTree(this.features);

            // default selection: first feature
            if (!features.isEmpty()) {
                this.selectedFeature = features.get(0);
                this.selectedTreeNode = featureIdToTreeNode.get(selectedFeature.id());

                if (this.selectedTreeNode != null) {
                    this.selectedTreeNode.setSelected(true);
                    expandParents(this.selectedTreeNode);
                }
            }

            log.info("RunDetails loaded: filter={}, item={}, run={}, dir={}, features={}",
                    filter, item, run, runDir, features.size());

        } catch (Exception e) {
            this.error = msg(e);
            log.warn("RunDetails failed to load: {}", this.error, e);

            this.features = List.of();
            this.selectedFeature = null;
            this.featureIdToTreeNode.clear();

            this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                    new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0, 0),
                    null
            );

            this.selectedTreeNode = null;
            this.recentRuns = List.of();
            this.runLabelsByFolder.clear();
            this.scenarioHistoryByKey.clear();
            this.featureFlakyCounts.clear();
            this.flakyScenarioCount = 0;
        }
    }

    // ---------------------- selection ----------------------

    /** p:tree select listener */
    public void onTreeSelect(NodeSelectEvent event) {
        syncSelectedFeatureFromSelection();
    }

    /** method expression without args */
    public void onTreeSelect() {
        syncSelectedFeatureFromSelection();
    }

    private void syncSelectedFeatureFromSelection() {
        if (selectedTreeNode == null || selectedTreeNode.getData() == null) return;
        TreeItem ti = selectedTreeNode.getData();

        // Only FEATURE nodes change the content pane
        if (ti.type() == TreeType.FEATURE && ti.featureId() != null) {
            this.selectedFeature = features.stream()
                    .filter(f -> Objects.equals(f.id(), ti.featureId()))
                    .findFirst()
                    .orElse(null);

            if (this.selectedFeature != null) {
                expandParents(this.selectedTreeNode);
            }
        }
    }

    private static void expandParents(TreeNode<?> node) {
        TreeNode<?> p = node == null ? null : node.getParent();
        while (p != null) {
            p.setExpanded(true);
            p = p.getParent();
        }
    }

    // ---------------------- resolution ----------------------

    private DashboardFiltersProperties.Item resolveItemConfig(String filterName, String itemTitle) {
        var f = props.getFilters().stream()
                .filter(x -> Objects.equals(x.getName(), filterName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown filter: " + filterName));

        return f.getItems().stream()
                .filter(x -> Objects.equals(x.getTitle(), itemTitle))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown item: " + itemTitle));
    }

    private Path resolveRunDir(DashboardFiltersProperties.Item item, String runFolder) {
        Path base = Path.of(item.getBaseDir()).normalize().toAbsolutePath();
        Path dir = base.resolve(runFolder).normalize().toAbsolutePath();

        if (!dir.startsWith(base)) {
            throw new IllegalArgumentException("Invalid run path (escape attempt)");
        }
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException("Run dir not found: " + dir);
        }
        return dir;
    }

    private ExtentSummary readSummary(Path p) throws Exception {
        if (!Files.exists(p)) {
            throw new IllegalArgumentException("Missing extent.summary.json: " + p);
        }
        return MAPPER.readValue(Files.readString(p), ExtentSummary.class);
    }

    // ---------------------- run history / flaky ----------------------

    private void loadRunHistory(DashboardFiltersProperties.Item itemConfig, Path summaryPath) {
        recentRuns = List.of();
        runLabelsByFolder.clear();
        scenarioHistoryByKey.clear();
        flakyScenarioCount = 0;

        if (itemConfig == null) return;

        Pattern p = Pattern.compile(itemConfig.getDirNameRegex());
        Path base = Path.of(itemConfig.getBaseDir());

        List<RunPicker.PickedRun> recent;
        try {
            recent = new ArrayList<>(runPicker.pickLatestRuns(base, p, HISTORY_LIMIT));
        } catch (Exception e) {
            recent = new ArrayList<>();
        }

        Path currentDir = runDir == null ? null : runDir.toAbsolutePath().normalize();
        boolean hasCurrent = currentDir != null && recent.stream()
                .anyMatch(r -> r.runDir().toAbsolutePath().normalize().equals(currentDir));

        if (!hasCurrent && runDir != null && summary != null) {
            try {
                long lm = Files.getLastModifiedTime(summaryPath).toMillis();
                recent.add(0, new RunPicker.PickedRun(runDir, summaryPath, lm, summary));
            } catch (Exception ignored) {
                // If we can't stat current run, keep the list as-is
            }
        }

        if (recent.size() > HISTORY_LIMIT) {
            recent = recent.subList(0, HISTORY_LIMIT);
        }

        List<RunOption> options = new ArrayList<>();
        for (RunPicker.PickedRun pr : recent) {
            String runFolder = pr.runDir().getFileName().toString();
            String label = formatRunLabel(pr);
            String tooltip = formatRunTooltip(pr);
            runLabelsByFolder.put(runFolder, label);
            options.add(new RunOption(runFolder, label, tooltip, Objects.equals(runFolder, run)));
        }
        recentRuns = options;

        Map<String, List<RunStatus>> history = new HashMap<>();
        for (RunPicker.PickedRun pr : recent) {
            String runFolder = pr.runDir().getFileName().toString();
            Path extent = pr.runDir().resolve("extent.json");
            try {
                Map<String, String> statuses = RunHistoryAnalyzer.parseScenarioStatuses(extent);
                for (Map.Entry<String, String> entry : statuses.entrySet()) {
                    history.computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                            .add(new RunStatus(runFolder, entry.getValue()));
                }
            } catch (Exception ignored) {
                // Ignore bad runs when building history
            }
        }

        for (Map.Entry<String, List<RunStatus>> entry : history.entrySet()) {
            List<RunStatus> statuses = entry.getValue();
            boolean flaky = RunHistoryAnalyzer.isFlaky(
                    statuses.stream().map(RunStatus::status).toList()
            );
            scenarioHistoryByKey.put(entry.getKey(), new ScenarioHistory(statuses, flaky));
            if (flaky) flakyScenarioCount++;
        }
    }

    private void computeFeatureFlakyCounts() {
        featureFlakyCounts.clear();
        if (features == null || features.isEmpty()) return;

        for (FeatureModel f : features) {
            int count = 0;
            for (ScenarioModel sc : f.scenarios()) {
                if (isScenarioFlaky(f, sc)) {
                    count++;
                }
            }
            featureFlakyCounts.put(f.id(), count);
        }
    }

    private boolean isScenarioFlaky(FeatureModel feature, ScenarioModel scenario) {
        String key = scenarioKeyFor(feature, scenario);
        ScenarioHistory history = scenarioHistoryByKey.get(key);
        return history != null && history.flaky();
    }

    private String scenarioKeyFor(FeatureModel feature, ScenarioModel scenario) {
        List<String> fullPath = new ArrayList<>();
        if (feature.groupPath() != null) {
            fullPath.addAll(feature.groupPath());
        }
        fullPath.add(feature.title());
        return RunHistoryAnalyzer.scenarioKey(fullPath, scenario.name());
    }

    private String formatRunLabel(RunPicker.PickedRun pr) {
        if (pr == null || pr.summary() == null || pr.summary().run() == null) {
            return pr == null ? "" : pr.runDir().getFileName().toString();
        }
        LocalDateTime start = parseExtent(pr.summary().run().startTime());
        if (start != null) {
            return start.format(RUN_LABEL_FMT);
        }
        return pr.runDir().getFileName().toString();
    }

    private static String formatRunTooltip(RunPicker.PickedRun pr) {
        if (pr == null || pr.summary() == null || pr.summary().totals() == null) {
            return pr == null ? "" : pr.runDir().getFileName().toString();
        }
        ExtentSummary.Totals t = pr.summary().totals();
        return "Pass " + t.pass()
                + " | Fail " + t.fail()
                + " | Known Bug " + t.knownBug()
                + " | Skip " + t.skip();
    }

    // ---------------------- parsing: extent.json -> model ----------------------

    private List<FeatureModel> parseExtentToModel(Path extentJson) throws Exception {
        if (!Files.exists(extentJson)) {
            throw new IllegalArgumentException("Missing extent.json: " + extentJson);
        }

        JsonNode rootArr = MAPPER.readTree(Files.readString(extentJson));
        if (rootArr == null || !rootArr.isArray()) {
            throw new IllegalStateException("extent.json root is not an array (expected Feature[]).");
        }

        List<FeatureModel> out = new ArrayList<>();

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

            if (fullPath.isEmpty() && !featureTitle.isBlank()) {
                fullPath = List.of(featureTitle);
            }

            List<String> groupsOnly = fullPath.size() <= 1
                    ? List.of()
                    : fullPath.subList(0, fullPath.size() - 1);

            String status = f.path("status").asText("");
            LocalDateTime st = parseExtent(f.path("startTime").asText(""));
            LocalDateTime et = parseExtent(f.path("endTime").asText(""));
            String durationText = formatDurationSafe(st, et);
            List<String> featureTags = readStringArray(f.get("categorySet"));

            List<ScenarioModel> scenarios = new ArrayList<>();
            JsonNode scenArr = f.get("children");
            if (scenArr != null && scenArr.isArray()) {
                for (JsonNode s : scenArr) {
                    if (!s.isObject()) continue;
                    String sType = s.path("bddType").asText("");
                    if (!sType.endsWith(".Scenario")) continue;

                    String sName = pickName(s);
                    String sStatus = s.path("status").asText("");
                    LocalDateTime sSt = parseExtent(s.path("startTime").asText(""));
                    LocalDateTime sEt = parseExtent(s.path("endTime").asText(""));
                    String sDur = formatDurationSafe(sSt, sEt);

                    List<String> tags = readStringArray(s.get("categorySet"));

                    List<StepModel> steps = new ArrayList<>();
                    JsonNode stepArr = s.get("children");
                    if (stepArr != null && stepArr.isArray()) {
                        for (JsonNode step : stepArr) {
                            if (!step.isObject()) continue;

                            String stepType = step.path("bddType").asText("");
                            String rawStepName = pickName(step);
                            String stepDesc = step.path("description").asText("");

                            StepLabel lbl = splitStepLabel(stepType, rawStepName);

                            String stepStatus = step.path("status").asText("");
                            LocalDateTime stepSt = parseExtent(step.path("startTime").asText(""));
                            LocalDateTime stepEt = parseExtent(step.path("endTime").asText(""));
                            List<LogEntry> logs = readLogs(step.get("logs"));
                            String stepDur = stepDurationText(logs, stepSt, stepEt);

                            // If any log has media, show as step "preview" too
                            String stepMedia = logs.stream()
                                    .map(LogEntry::mediaPath)
                                    .filter(p -> p != null && !p.isBlank())
                                    .findFirst()
                                    .orElse(null);

                            if (isAfterStep(stepDesc) && !steps.isEmpty()) {
                                StepModel prev = steps.remove(steps.size() - 1);
                                List<LogEntry> mergedLogs = new ArrayList<>(prev.logs());
                                mergedLogs.addAll(logs);
                                String mergedMedia = !isBlank(prev.screenshotPath()) ? prev.screenshotPath() : stepMedia;
                                String mergedDur = stepDurationText(mergedLogs, null, null);
                                if (isBlank(mergedDur) || "—".equals(mergedDur)) {
                                    mergedDur = prev.durationText();
                                }

                                steps.add(new StepModel(
                                        prev.id(),
                                        prev.keyword(),
                                        prev.text(),
                                        prev.status(),
                                        mergedDur,
                                        mergedLogs,
                                        mergedMedia
                                ));
                                continue;
                            }

                            steps.add(new StepModel(
                                    UUID.randomUUID().toString(),
                                    lbl.keyword(),
                                    lbl.text(),
                                    stepStatus,
                                    stepDur,
                                    logs,
                                    stepMedia
                            ));
                        }
                    }

                    scenarios.add(new ScenarioModel(
                            UUID.randomUUID().toString(),
                            sName,
                            sStatus,
                            sDur,
                            tags,
                            steps
                    ));
                }
            }

            out.add(new FeatureModel(
                    UUID.randomUUID().toString(),
                    groupsOnly,
                    featureTitle,
                    status,
                    durationText,
                    featureTags,
                    scenarios
            ));
        }

        return out;
    }

    private static String pickName(JsonNode n) {
        String dn = n.path("displayName").asText("");
        if (dn != null && !dn.isBlank()) return dn;
        return n.path("name").asText("");
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

    private static List<String> readStringArray(JsonNode arr) {
        if (arr == null || !arr.isArray()) return List.of();
        List<String> out = new ArrayList<>();
        for (JsonNode x : arr) if (x.isTextual() && !x.asText().isBlank()) out.add(x.asText());
        return out;
    }

    private static List<LogEntry> readLogs(JsonNode logsArr) {
        if (logsArr == null || !logsArr.isArray()) return List.of();

        List<LogEntry> out = new ArrayList<>();
        for (JsonNode l : logsArr) {
            String ts = l.path("timestamp").asText("");
            String st = l.path("status").asText("");
            String rawDetails = l.path("details").asText("");
            long durationMillis = extractDurationMillis(rawDetails);
            String durationText = durationMillis >= 0 ? formatStepDuration(Duration.ofMillis(durationMillis)) : "";
            String details = stripExtentTimestamps(rawDetails);

            String mediaPath = null;
            JsonNode media = l.get("media");
            if (media != null && media.isObject()) {
                String p = media.path("path").asText(null);
                if (p != null && !p.isBlank()) mediaPath = p;
            }

            out.add(new LogEntry(ts, st, details, mediaPath, durationText, durationMillis));
        }
        return out;
    }

    // We want Hebrew keyword display (like Extent in your screenshot) BUT strip English prefixes from name (When/And/etc).
    private static StepLabel splitStepLabel(String bddType, String rawName) {
        KeywordPair kp = keywordPairFromBddType(bddType);
        String text = rawName == null ? "" : rawName.trim();

        // Strip English first (because JSON name usually starts with "When ...")
        if (!kp.english().isBlank() && text.startsWith(kp.english() + " ")) {
            text = text.substring(kp.english().length() + 1).trim();
        } else if (!kp.hebrew().isBlank() && text.startsWith(kp.hebrew() + " ")) {
            text = text.substring(kp.hebrew().length() + 1).trim();
        } else {
            // fallback: strip common English keywords if present
            for (String k : List.of("Given", "When", "Then", "And", "But")) {
                if (text.startsWith(k + " ")) {
                    text = text.substring(k.length() + 1).trim();
                    break;
                }
            }
        }

        String keyword = !kp.hebrew().isBlank() ? kp.hebrew() : kp.english();
        if (keyword == null) keyword = "";
        return new StepLabel(keyword, text);
    }

    private static KeywordPair keywordPairFromBddType(String bddType) {
        if (bddType == null) return new KeywordPair("", "");
        if (bddType.endsWith(".Given")) return new KeywordPair("Given", "בהינתן");
        if (bddType.endsWith(".When"))  return new KeywordPair("When",  "כאשר");
        if (bddType.endsWith(".Then"))  return new KeywordPair("Then",  "אז");
        if (bddType.endsWith(".And"))   return new KeywordPair("And",   "וגם");
        if (bddType.endsWith(".But"))   return new KeywordPair("But",   "אבל");
        if (bddType.endsWith(".Asterisk")) return new KeywordPair("*",  "צעד");
        return new KeywordPair("", "");
    }

    private static boolean isAfterStep(String description) {
        return "AFTER_STEP".equalsIgnoreCase(String.valueOf(description).trim());
    }

    private record KeywordPair(String english, String hebrew) {}

    // ---------------------- tree build ----------------------

    private void buildFeatureTree(List<FeatureModel> features) {
        this.featureIdToTreeNode.clear();
        Map<String, TreeNode<TreeItem>> groupNodes = new HashMap<>();
        Map<String, int[]> groupCounts = new HashMap<>();
        Map<String, Integer> groupFlakyCounts = new HashMap<>();

        for (FeatureModel f : features) {
            if (f.groupPath() == null || f.groupPath().isEmpty()) continue;
            StringBuilder key = new StringBuilder();
            int featureFlaky = featureFlakyCounts.getOrDefault(f.id(), 0);
            for (int i = 0; i < f.groupPath().size(); i++) {
                if (i > 0) key.append(" / ");
                key.append(f.groupPath().get(i));
                addGroupCount(groupCounts, key.toString(), f.status());
                if (featureFlaky > 0) {
                    groupFlakyCounts.merge(key.toString(), featureFlaky, Integer::sum);
                }
            }
        }

        // IMPORTANT:
        // If your XHTML uses <p:treeNode type="GROUP"> and <p:treeNode type="FEATURE">,
        // then nodes MUST be created with matching "type" strings or they will render blank.
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0, 0),
                null
        );

        for (FeatureModel f : features) {
            TreeNode<TreeItem> parent = featureTreeRoot;

            StringBuilder key = new StringBuilder();
            for (int i = 0; i < f.groupPath().size(); i++) {
                if (i > 0) key.append(" / ");
                key.append(f.groupPath().get(i));
                parent = ensureGroupNode(parent, f.groupPath().get(i), key.toString(), groupNodes, groupCounts, groupFlakyCounts);
            }

            int featureFlaky = featureFlakyCounts.getOrDefault(f.id(), 0);
            TreeItem leaf = new TreeItem(TreeType.FEATURE, f.title(), f.status(), f.id(), f.tags(), 0, 0, 0, 0, featureFlaky);
            TreeNode<TreeItem> leafNode = new DefaultTreeNode<>("FEATURE", leaf, parent);

            featureIdToTreeNode.put(f.id(), leafNode);
        }

        // Expand root by default (doesn't hurt, helps UX)
        featureTreeRoot.setExpanded(true);
    }

    private TreeNode<TreeItem> ensureGroupNode(
            TreeNode<TreeItem> parent,
            String label,
            String pathKey,
            Map<String, TreeNode<TreeItem>> groupNodes,
            Map<String, int[]> groupCounts,
            Map<String, Integer> groupFlakyCounts
    ) {
        TreeNode<TreeItem> existing = groupNodes.get(pathKey);
        if (existing != null) {
            return existing;
        }
        int[] counts = groupCounts.getOrDefault(pathKey, new int[4]);
        int flakyCount = groupFlakyCounts.getOrDefault(pathKey, 0);
        TreeNode<TreeItem> created = new DefaultTreeNode<>("GROUP",
                new TreeItem(
                        TreeType.GROUP,
                        label,
                        null,
                        null,
                        List.of(),
                        counts[0],
                        counts[1],
                        counts[2],
                        counts[3],
                        flakyCount
                ),
                parent
        );
        created.setSelectable(false);
        created.setExpanded(true);
        groupNodes.put(pathKey, created);
        return created;
    }

    private static void addGroupCount(Map<String, int[]> counts, String key, String status) {
        if (key == null || key.isBlank()) return;
        int[] c = counts.computeIfAbsent(key, k -> new int[4]); // pass, fail, knownBug, skip
        String st = String.valueOf(status).toUpperCase(Locale.ROOT);
        switch (st) {
            case "PASS" -> c[0]++;
            case "FAIL" -> c[1]++;
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> c[2]++;
            case "SKIP" -> c[3]++;
            default -> { }
        }
    }

    // ---------------------- UI helpers ----------------------

    public String statusCss(String status) {
        if (status == null) return "st-unknown";
        return switch (status.toUpperCase()) {
            case "PASS" -> "st-pass";
            case "FAIL" -> "st-fail";
            case "SKIP" -> "st-skip";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "st-knownbug";
            case "INFO" -> "st-info";
            default -> "st-unknown";
        };
    }

    public String statusHebrew(String status) {
        if (status == null) return "";
        return switch (status.toUpperCase()) {
            case "PASS" -> "עבר בהצלחה";
            case "FAIL" -> "נכשל";
            case "SKIP" -> "דולג";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "באג ידוע";
            case "INFO" -> "מידע";
            default -> status;
        };
    }

    public String statusBadgeLabel(String status) {
        if (status == null) return "";
        return switch (status.toUpperCase()) {
            case "PASS" -> "Pass";
            case "FAIL" -> "Fail";
            case "SKIP" -> "Skip";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "Known Bug";
            case "INFO" -> "Info";
            default -> status;
        };
    }

    public String statusIconClass(String status) {
        if (status == null) return "fa-regular fa-circle";
        return switch (status.toUpperCase()) {
            case "PASS" -> "fa-solid fa-circle-check";
            case "FAIL" -> "fa-solid fa-circle-xmark";
            case "SKIP" -> "fa-solid fa-circle-minus";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "fa-solid fa-triangle-exclamation";
            case "INFO" -> "fa-solid fa-circle-info";
            default -> "fa-regular fa-circle";
        };
    }

    public String statusIconTone(String status) {
        if (status == null) return "ico-unknown";
        return switch (status.toUpperCase()) {
            case "PASS" -> "ico-pass";
            case "FAIL" -> "ico-fail";
            case "SKIP" -> "ico-skip";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "ico-knownbug";
            case "INFO" -> "ico-info";
            default -> "ico-unknown";
        };
    }

    public String statusShortLabel(String status) {
        if (status == null) return "?";
        return switch (status.toUpperCase()) {
            case "PASS" -> "P";
            case "FAIL" -> "F";
            case "SKIP" -> "S";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "K";
            case "INFO" -> "I";
            default -> "?";
        };
    }

    public String runDurationText() {
        if (summary == null || summary.run() == null) return "—";
        LocalDateTime st = parseExtent(summary.run().startTime());
        LocalDateTime et = parseExtent(summary.run().endTime());
        return formatDurationSafe(st, et);
    }

    public boolean hasHistory() { return recentRuns != null && !recentRuns.isEmpty(); }

    public boolean isHasHistory() { return hasHistory(); }

    public List<RunOption> getRecentRuns() { return recentRuns; }

    public String runLabel(String runFolder) {
        if (runFolder == null) return "";
        return runLabelsByFolder.getOrDefault(runFolder, runFolder);
    }

    public List<RunStatus> getScenarioHistory(ScenarioModel sc) {
        if (selectedFeature == null || sc == null) return List.of();
        String key = scenarioKeyFor(selectedFeature, sc);
        ScenarioHistory history = scenarioHistoryByKey.get(key);
        return history == null ? List.of() : history.statuses();
    }

    public List<RunStatus> scenarioHistory(ScenarioModel sc) {
        return getScenarioHistory(sc);
    }

    public boolean isScenarioFlaky(ScenarioModel sc) {
        if (selectedFeature == null || sc == null) return false;
        return isScenarioFlaky(selectedFeature, sc);
    }

    public boolean scenarioFlaky(ScenarioModel sc) {
        return isScenarioFlaky(sc);
    }

    public int getSelectedFeatureFlakyCount() {
        if (selectedFeature == null) return 0;
        return featureFlakyCounts.getOrDefault(selectedFeature.id(), 0);
    }

    public String getSelectedFeatureFullTitle() {
        if (selectedFeature == null) return "";
        List<String> parts = new ArrayList<>(selectedFeature.groupPath());
        parts.add(selectedFeature.title());
        return String.join(" ← ", parts);
    }

    public String assetUrl(String relativePath) {
        if (relativePath == null || relativePath.isBlank()) return "";
        return "/run-asset?filter=" + enc(filter)
               + "&item=" + enc(item)
               + "&run=" + enc(run)
               + "&path=" + enc(relativePath);
    }

    public List<String> getAllTags() {
        if (features == null || features.isEmpty()) return List.of();
        Set<String> out = new TreeSet<>();
        for (FeatureModel f : features) {
            if (f.tags() == null) continue;
            for (String t : f.tags()) {
                if (t != null && !t.isBlank()) out.add(t);
            }
        }
        return new ArrayList<>(out);
    }

    public String joinTags(List<String> tags) {
        if (tags == null || tags.isEmpty()) return "";
        return String.join(" ", tags);
    }

    // ---------------------- time helpers ----------------------

    private static LocalDateTime parseExtent(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.replace('\u202F', ' ')  // narrow no-break space
                .replace('\u00A0', ' ')       // no-break space
                .replaceAll("\\s+", " ")
                .trim();
        try {
            return LocalDateTime.parse(s, EXTENT_TIME_FMT);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String formatDurationSafe(LocalDateTime st, LocalDateTime et) {
        if (st == null || et == null) return "—";
        Duration d = Duration.between(st, et);
        if (d.isNegative()) return "—";
        return formatDuration(d);
    }

    private static String formatDuration(Duration d) {
        long sec = Math.max(0, d.getSeconds());
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        return (h > 0) ? (h + ":" + two(m) + ":" + two(s)) : (m + ":" + two(s));
    }

    private static String two(long n) { return n < 10 ? "0" + n : Long.toString(n); }

    private static String stepDurationText(List<LogEntry> logs, LocalDateTime st, LocalDateTime et) {
        Duration sum = sumLogDurations(logs);
        if (sum != null) {
            return formatStepDuration(sum);
        }
        if (st == null || et == null) return "—";
        Duration d = Duration.between(st, et);
        if (d.isNegative()) return "—";
        return formatStepDuration(d);
    }

    private static Duration sumLogDurations(List<LogEntry> logs) {
        if (logs == null || logs.isEmpty()) return null;
        long totalMillis = 0;
        boolean found = false;
        for (LogEntry lg : logs) {
            long ms = lg.durationMillis();
            if (ms >= 0) {
                found = true;
                totalMillis += ms;
            }
        }
        return found ? Duration.ofMillis(totalMillis) : null;
    }

    private static long extractDurationMillis(String detailsHtml) {
        if (detailsHtml == null || detailsHtml.isBlank()) return -1;
        Matcher m = LOG_TS_PATTERN.matcher(detailsHtml);
        long total = 0;
        boolean found = false;
        while (m.find()) {
            long ms = parseDurationMillis(m.group(1));
            if (ms >= 0) {
                total += ms;
                found = true;
            }
        }
        return found ? total : -1;
    }

    private static long parseDurationMillis(String raw) {
        if (raw == null) return -1;
        String[] parts = raw.trim().split(":");
        try {
            if (parts.length == 2) {
                long min = Long.parseLong(parts[0]);
                long sec = Long.parseLong(parts[1]);
                return (min * 60 + sec) * 1000;
            }
            if (parts.length == 3) {
                long hrs = Long.parseLong(parts[0]);
                long min = Long.parseLong(parts[1]);
                long sec = Long.parseLong(parts[2]);
                return (hrs * 3600 + min * 60 + sec) * 1000;
            }
            if (parts.length == 4) {
                long hrs = Long.parseLong(parts[0]);
                long min = Long.parseLong(parts[1]);
                long sec = Long.parseLong(parts[2]);
                long ms = Long.parseLong(parts[3]);
                return (hrs * 3600 + min * 60 + sec) * 1000 + ms;
            }
        } catch (NumberFormatException ignored) {
            return -1;
        }
        return -1;
    }

    private static String formatStepDuration(Duration d) {
        long totalSeconds = Math.max(0, (long) Math.floor(d.toMillis() / 1000.0));
        long minutes = totalSeconds / 60;
        long seconds = totalSeconds % 60;
        return String.format(Locale.ROOT, "%02d:%02d", minutes, seconds);
    }

    private static String stripExtentTimestamps(String html) {
        if (html == null || html.isBlank()) return "";
        String cleaned = LOG_TS_PATTERN.matcher(html).replaceAll("");
        cleaned = LOG_LOCALTIME_PATTERN.matcher(cleaned).replaceAll("");
        return cleaned.trim();
    }

    // ---------------------- misc helpers ----------------------

    private static String enc(String s) {
        return URLEncoder.encode(String.valueOf(s), StandardCharsets.UTF_8);
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static String msg(Exception e) {
        return (e.getMessage() == null || e.getMessage().isBlank())
                ? e.getClass().getSimpleName()
                : e.getMessage();
    }

    // ---------------------- getters/setters ----------------------

    public String getFilter() { return filter; }
    public void setFilter(String filter) { this.filter = filter; }

    public String getItem() { return item; }
    public void setItem(String item) { this.item = item; }

    public String getRun() { return run; }
    public void setRun(String run) { this.run = run; }

    public String getError() { return error; }
    public Path getRunDir() { return runDir; }
    public ExtentSummary getSummary() { return summary; }

    public TreeNode<TreeItem> getFeatureTreeRoot() { return featureTreeRoot; }

    public TreeNode<TreeItem> getSelectedTreeNode() { return selectedTreeNode; }
    public void setSelectedTreeNode(TreeNode<TreeItem> selectedTreeNode) { this.selectedTreeNode = selectedTreeNode; }

    public FeatureModel getSelectedFeature() { return selectedFeature; }

    public List<FeatureModel> getFeatures() { return features; }

    // ---------------------- model types ----------------------

    public enum TreeType { ROOT, GROUP, FEATURE }

    public record TreeItem(
            TreeType type,
            String label,
            String status,
            String featureId,
            List<String> tags,
            int passCount,
            int failCount,
            int knownBugCount,
            int skipCount,
            int flakyCount
    ) implements Serializable {}

    public record RunOption(
            String runFolder,
            String label,
            String tooltip,
            boolean current
    ) implements Serializable {}

    public record RunStatus(
            String runFolder,
            String status
    ) implements Serializable {}

    public record ScenarioHistory(
            List<RunStatus> statuses,
            boolean flaky
    ) implements Serializable {}

    public record FeatureModel(
            String id,
            List<String> groupPath,
            String title,
            String status,
            String durationText,
            List<String> tags,
            List<ScenarioModel> scenarios
    ) implements Serializable {}

    public record ScenarioModel(
            String id,
            String name,
            String status,
            String durationText,
            List<String> tags,
            List<StepModel> steps
    ) implements Serializable {}

    public record StepModel(
            String id,
            String keyword,
            String text,
            String status,
            String durationText,
            List<LogEntry> logs,
            String screenshotPath
    ) implements Serializable {}

    public record LogEntry(
            String timestamp,
            String status,
            String detailsHtml,
            String mediaPath,
            String durationText,
            long durationMillis
    ) implements Serializable {}

    private record StepLabel(String keyword, String text) {}
}
