package com.leumit.dashboard.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumit.dashboard.config.DashboardFiltersProperties;
import com.leumit.dashboard.model.ExtentSummary;
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

@Slf4j
@Component("runDetailsView")
@Scope("view")
public class RunDetailsView implements Serializable {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    // Extent time strings look like: "Jan 14, 2026, 9:28:24 AM" (note narrow no-break space)
    private static final DateTimeFormatter EXTENT_TIME_FMT =
            DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm:ss a", Locale.ENGLISH);

    private final DashboardFiltersProperties props;

    // view params
    private String filter;
    private String item;
    private String run;

    private boolean loaded;
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

    public RunDetailsView(DashboardFiltersProperties props) {
        this.props = props;
        // IMPORTANT: use typed root so <p:treeNode type="ROOT"> can match if you define it (optional)
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null),
                null
        );
    }

    /**
     * Called from <f:event type="preRenderView" .../>
     * Runs AFTER <f:viewParam> is applied.
     */
    public void preRender() {
        if (loaded) return;
        loaded = true;

        try {
            if (isBlank(filter) || isBlank(item) || isBlank(run)) {
                throw new IllegalArgumentException("Missing URL params: filter/item/run");
            }

            this.runDir = resolveRunDir(filter, item, run);

            Path summaryPath = runDir.resolve("extent.summary.json");
            this.summary = readSummary(summaryPath);

            Path extentPath = runDir.resolve("extent.json");
            this.features = parseExtentToModel(extentPath);

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
                    new TreeItem(TreeType.ROOT, "ROOT", null, null),
                    null
            );

            this.selectedTreeNode = null;
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

    private Path resolveRunDir(String filterName, String itemTitle, String runFolder) {
        var f = props.getFilters().stream()
                .filter(x -> Objects.equals(x.getName(), filterName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown filter: " + filterName));

        var it = f.getItems().stream()
                .filter(x -> Objects.equals(x.getTitle(), itemTitle))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown item: " + itemTitle));

        Path base = Path.of(it.getBaseDir()).normalize().toAbsolutePath();
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

            String displayName = pickName(f);
            List<String> groupPath = parseArrowPath(displayName);
            String featureTitle = groupPath.isEmpty() ? displayName : groupPath.get(groupPath.size() - 1);
            List<String> groupsOnly = groupPath.size() <= 1 ? List.of() : groupPath.subList(0, groupPath.size() - 1);

            String status = f.path("status").asText("");
            LocalDateTime st = parseExtent(f.path("startTime").asText(""));
            LocalDateTime et = parseExtent(f.path("endTime").asText(""));
            String durationText = formatDurationSafe(st, et);

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

                            StepLabel lbl = splitStepLabel(stepType, rawStepName);

                            String stepStatus = step.path("status").asText("");
                            LocalDateTime stepSt = parseExtent(step.path("startTime").asText(""));
                            LocalDateTime stepEt = parseExtent(step.path("endTime").asText(""));
                            String stepDur = formatDurationSafe(stepSt, stepEt);

                            List<LogEntry> logs = readLogs(step.get("logs"));

                            // If any log has media, show as step "preview" too
                            String stepMedia = logs.stream()
                                    .map(LogEntry::mediaPath)
                                    .filter(p -> p != null && !p.isBlank())
                                    .findFirst()
                                    .orElse(null);

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

    private record KeywordPair(String english, String hebrew) {}

    // ---------------------- tree build ----------------------

    private void buildFeatureTree(List<FeatureModel> features) {
        this.featureIdToTreeNode.clear();

        // IMPORTANT:
        // If your XHTML uses <p:treeNode type="GROUP"> and <p:treeNode type="FEATURE">,
        // then nodes MUST be created with matching "type" strings or they will render blank.
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null),
                null
        );

        for (FeatureModel f : features) {
            TreeNode<TreeItem> parent = featureTreeRoot;

            for (String g : f.groupPath()) {
                parent = ensureGroupNode(parent, g);
            }

            TreeItem leaf = new TreeItem(TreeType.FEATURE, f.title(), f.status(), f.id());
            TreeNode<TreeItem> leafNode = new DefaultTreeNode<>("FEATURE", leaf, parent);

            featureIdToTreeNode.put(f.id(), leafNode);
        }

        // Expand root by default (doesn't hurt, helps UX)
        featureTreeRoot.setExpanded(true);
    }

    private TreeNode<TreeItem> ensureGroupNode(TreeNode<TreeItem> parent, String label) {
        for (TreeNode<TreeItem> ch : parent.getChildren()) {
            TreeItem d = ch.getData();
            if (d != null && d.type() == TreeType.GROUP && Objects.equals(d.label(), label)) {
                return ch;
            }
        }
        TreeNode<TreeItem> created = new DefaultTreeNode<>("GROUP",
                new TreeItem(TreeType.GROUP, label, null, null),
                parent
        );
        created.setExpanded(true);
        return created;
    }

    // ---------------------- UI helpers ----------------------

    public String statusCss(String status) {
        if (status == null) return "st-unknown";
        return switch (status.toUpperCase()) {
            case "PASS" -> "st-pass";
            case "FAIL" -> "st-fail";
            case "SKIP" -> "st-skip";
            case "WARNING" -> "st-knownbug";
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
            case "WARNING" -> "באג ידוע";
            case "INFO" -> "מידע";
            default -> status;
        };
    }

    public String runDurationText() {
        if (summary == null || summary.run() == null) return "—";
        LocalDateTime st = parseExtent(summary.run().startTime());
        LocalDateTime et = parseExtent(summary.run().endTime());
        return formatDurationSafe(st, et);
    }

    public String assetUrl(String relativePath) {
        if (relativePath == null || relativePath.isBlank()) return "";
        return "/run-asset?filter=" + enc(filter)
               + "&item=" + enc(item)
               + "&run=" + enc(run)
               + "&path=" + enc(relativePath);
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
            String featureId
    ) implements Serializable {}

    public record FeatureModel(
            String id,
            List<String> groupPath,
            String title,
            String status,
            String durationText,
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
            String mediaPath
    ) implements Serializable {}

    private record StepLabel(String keyword, String text) {}
}
