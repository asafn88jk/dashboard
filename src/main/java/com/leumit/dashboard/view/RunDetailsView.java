package com.leumit.dashboard.view;

import com.leumit.dashboard.config.DashboardFiltersProperties;
import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.repo.RunPicker;
import com.leumit.dashboard.run.RunHistoryAnalyzer;
import com.leumit.dashboard.run.ReportCache;
import com.leumit.dashboard.run.ReportCutoff;
import com.leumit.dashboard.run.SparkHtmlReportParser;
import com.leumit.dashboard.run.SparkHtmlReportParser.Feature;
import com.leumit.dashboard.run.SparkHtmlReportParser.Log;
import com.leumit.dashboard.run.SparkHtmlReportParser.ParsedReport;
import com.leumit.dashboard.run.SparkHtmlReportParser.Scenario;
import com.leumit.dashboard.run.SparkHtmlReportParser.Step;
import lombok.extern.slf4j.Slf4j;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.primefaces.event.NodeSelectEvent;
import org.primefaces.model.menu.DefaultMenuItem;
import org.primefaces.model.menu.DefaultMenuModel;
import org.primefaces.model.menu.MenuModel;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component("runDetailsView")
@Scope("view")
public class RunDetailsView implements Serializable {

    private static final List<DateTimeFormatter> EXTENT_TIME_FMTS = List.of(
            DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MM.dd.yyyy h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M.d.yyyy h:mm:ss a", Locale.ENGLISH)
    );
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
    private static final Pattern LOG_LOCALTIME_VALUE_PATTERN = Pattern.compile(
            "<p[^>]*class=['\"]localtime['\"][^>]*>([^<]+)</p>",
            Pattern.CASE_INSENSITIVE
    );
    private static final String TABLE_BORDERED_CLASS = "table-bordered";
    private static final int HISTORY_LIMIT = 4;

    private final DashboardFiltersProperties props;
    private final RunPicker runPicker;
    private final ReportCache reportCache;

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

    // history
    private List<RunOption> recentRuns = List.of();
    private final Map<String, String> runLabelsByFolder = new HashMap<>();
    private final Map<String, List<RunStatus>> scenarioHistoryByKey = new HashMap<>();
    private MenuModel menubarModel = new DefaultMenuModel();
    private String runMenuLabel = "בחירת תאריך";

    public RunDetailsView(DashboardFiltersProperties props, RunPicker runPicker, ReportCache reportCache) {
        this.props = props;
        this.runPicker = runPicker;
        this.reportCache = reportCache;
        // IMPORTANT: use typed root so <p:treeNode type="ROOT"> can match if you define it (optional)
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0),
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
            long totalStartNs = System.nanoTime();
            this.error = null;
            if (isBlank(filter) || isBlank(item) || isBlank(run)) {
                throw new IllegalArgumentException("Missing URL params: filter/item/run");
            }

            DashboardFiltersProperties.Item itemConfig = resolveItemConfig(filter, item);
            this.runDir = resolveRunDir(itemConfig, run);

            Path reportPath = SparkHtmlReportParser.requireReportHtml(runDir);
            long reportStartNs = System.nanoTime();
            ParsedReport report = SparkHtmlReportParser.parseReport(reportPath);
            long reportMs = (System.nanoTime() - reportStartNs) / 1_000_000;
            this.summary = report.summary();
            LocalDate reportDate = SparkHtmlReportParser.resolveReportDate(this.summary, reportPath).orElse(null);
            if (reportDate != null && reportDate.isBefore(ReportCutoff.CUTOFF_DATE)) {
                throw new IllegalArgumentException("Report before cutoff date: " + reportDate);
            }
            this.features = mapReportFeatures(report.features());

            long historyStartNs = System.nanoTime();
            loadRunHistory(itemConfig, reportPath);
            long historyMs = (System.nanoTime() - historyStartNs) / 1_000_000;

            long treeStartNs = System.nanoTime();
            buildFeatureTree(this.features);
            long treeMs = (System.nanoTime() - treeStartNs) / 1_000_000;

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
            long totalMs = (System.nanoTime() - totalStartNs) / 1_000_000;
            log.info("RunDetails timings: reportMs={} historyMs={} treeMs={} totalMs={}",
                    reportMs, historyMs, treeMs, totalMs);

        } catch (Exception e) {
            this.error = msg(e);
            log.warn("RunDetails failed to load: {}", this.error, e);

            this.features = List.of();
            this.selectedFeature = null;
            this.featureIdToTreeNode.clear();

            this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                    new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0),
                    null
            );

            this.selectedTreeNode = null;
            this.recentRuns = List.of();
            this.runLabelsByFolder.clear();
            this.scenarioHistoryByKey.clear();
            buildMenubarModel();
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

    // ---------------------- run history ----------------------

    private void loadRunHistory(DashboardFiltersProperties.Item itemConfig, Path reportPath) {
        recentRuns = List.of();
        runLabelsByFolder.clear();
        scenarioHistoryByKey.clear();

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
                long lm = Files.getLastModifiedTime(reportPath).toMillis();
                recent.add(0, new RunPicker.PickedRun(runDir, reportPath, lm, summary));
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
        buildMenubarModel();

        Path currentReportPath = reportPath == null ? null : reportPath.toAbsolutePath().normalize();
        Map<String, String> currentStatuses = currentReportPath == null
                ? Map.of()
                : buildScenarioStatusesFromFeatures();

        List<String> runOrder = new ArrayList<>();
        Map<String, Map<String, String>> statusesByRun = new LinkedHashMap<>();
        for (RunPicker.PickedRun pr : recent) {
            String runFolder = pr.runDir().getFileName().toString();
            runOrder.add(runFolder);
            try {
                Map<String, String> statuses;
                Path runReportPath = pr.reportPath() == null
                        ? null
                        : pr.reportPath().toAbsolutePath().normalize();
                if (currentReportPath != null && currentReportPath.equals(runReportPath)) {
                    statuses = currentStatuses;
                } else {
                    statuses = reportCache.getScenarioStatuses(pr.reportPath());
                }
                statusesByRun.put(runFolder, statuses);
            } catch (Exception ignored) {
                statusesByRun.put(runFolder, Map.of());
            }
        }

        Set<String> allScenarioKeys = new LinkedHashSet<>();
        for (Map<String, String> statuses : statusesByRun.values()) {
            allScenarioKeys.addAll(statuses.keySet());
        }

        for (String key : allScenarioKeys) {
            List<RunStatus> statuses = new ArrayList<>();
            for (String runFolder : runOrder) {
                String status = statusesByRun.getOrDefault(runFolder, Map.of()).get(key);
                if (status == null || status.isBlank()) {
                    status = "UNKNOWN";
                }
                statuses.add(new RunStatus(runFolder, status));
            }
            scenarioHistoryByKey.put(key, statuses);
        }
    }

    private Map<String, String> buildScenarioStatusesFromFeatures() {
        if (features == null || features.isEmpty()) return Map.of();
        Map<String, String> out = new HashMap<>();
        for (FeatureModel f : features) {
            if (f.scenarios() == null) continue;
            for (ScenarioModel sc : f.scenarios()) {
                String key = scenarioKeyFor(f, sc);
                if (!key.isBlank()) {
                    out.put(key, sc.status());
                }
            }
        }
        return out;
    }

    private String scenarioKeyFor(FeatureModel feature, ScenarioModel scenario) {
        List<String> fullPath = new ArrayList<>();
        if (feature.groupPath() != null) {
            fullPath.addAll(feature.groupPath());
        }
        fullPath.add(feature.title());
        String scenarioName = scenario.name();
        if (feature.scenarios() != null) {
            int totalSame = 0;
            int ordinal = 0;
            for (ScenarioModel sc : feature.scenarios()) {
                if (!Objects.equals(sc.name(), scenarioName)) continue;
                totalSame++;
                if (Objects.equals(sc.id(), scenario.id())) {
                    ordinal = totalSame;
                }
            }
            if (totalSame > 1 && ordinal > 0) {
                scenarioName = scenarioName + " #" + ordinal;
            }
        }
        return RunHistoryAnalyzer.scenarioKey(fullPath, scenarioName);
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

    // ---------------------- parsing: Spark HTML -> model ----------------------

    private List<FeatureModel> mapReportFeatures(List<Feature> reportFeatures) {
        if (reportFeatures == null || reportFeatures.isEmpty()) return List.of();

        List<FeatureModel> out = new ArrayList<>();

        for (Feature f : reportFeatures) {
            List<String> fullPath = f.path() == null ? List.of() : f.path();
            if (fullPath.isEmpty()) {
                fullPath = parseArrowPath(f.name());
            }

            String featureTitle = !fullPath.isEmpty()
                    ? fullPath.get(fullPath.size() - 1)
                    : (f.name() == null ? "" : f.name().trim());

            if (fullPath.isEmpty() && !featureTitle.isBlank()) {
                fullPath = List.of(featureTitle);
            }

            List<String> groupsOnly = fullPath.size() <= 1
                    ? List.of()
                    : fullPath.subList(0, fullPath.size() - 1);

            LocalDateTime st = parseExtent(f.startTime());
            LocalDateTime et = parseExtent(f.endTime());
            String durationText = formatDurationSafe(st, et);
            List<String> featureTags = f.tags() == null ? List.of() : f.tags();
            String descriptionHtml = f.descriptionHtml() == null ? "" : f.descriptionHtml();

            List<ScenarioModel> scenarios = new ArrayList<>();
            for (Scenario sc : f.scenarios()) {
                String scenarioStartTime = scenarioStartTimeFromSteps(sc.steps());
                List<StepModel> steps = new ArrayList<>();
                for (Step stp : sc.steps()) {
                    StepLabel lbl = splitStepLabelFromText(stp.text());
                    List<LogEntry> logs = mapLogs(stp.logs());
                    String stepDur = stepDurationText(logs, null, null);

                    String stepMedia = logs.stream()
                            .map(LogEntry::mediaPath)
                            .filter(p -> p != null && !p.isBlank())
                            .findFirst()
                            .orElse(null);

                    steps.add(new StepModel(
                            UUID.randomUUID().toString(),
                            lbl.keyword(),
                            lbl.text(),
                            stp.status(),
                            stepDur,
                            logs,
                            stepMedia
                    ));
                }

                scenarios.add(new ScenarioModel(
                        UUID.randomUUID().toString(),
                        sc.name(),
                        sc.status(),
                        scenarioStartTime,
                        scenarioDurationFromSteps(steps),
                        List.of(),
                        steps
                ));
            }

            out.add(new FeatureModel(
                    UUID.randomUUID().toString(),
                    groupsOnly,
                    featureTitle,
                    f.status(),
                    durationText,
                    featureTags,
                    descriptionHtml,
                    scenarios
            ));
        }

        return out;
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
    private static List<LogEntry> mapLogs(List<Log> logs) {
        if (logs == null || logs.isEmpty()) return List.of();

        List<LogEntry> out = new ArrayList<>();
        for (Log l : logs) {
            String rawDetails = l == null ? "" : (l.detailsHtml() == null ? "" : l.detailsHtml());
            long durationMillis = extractDurationMillis(rawDetails);
            String durationText = durationMillis >= 0 ? formatStepDuration(Duration.ofMillis(durationMillis)) : "";
            String cleaned = stripExtentTimestamps(rawDetails);
            ParsedLogHtml parsed = parseLogHtml(cleaned);
            String details = parsed.html();
            String mediaPath = l == null ? null : l.mediaPath();

            out.add(new LogEntry("", "", details, mediaPath, durationText, durationMillis, parsed.tables()));
        }
        return out;
    }

    private static ParsedLogHtml parseLogHtml(String html) {
        if (html == null || html.isBlank()) {
            return new ParsedLogHtml("", List.of());
        }
        if (!html.contains(TABLE_BORDERED_CLASS)) {
            return new ParsedLogHtml(html.trim(), List.of());
        }

        Document doc = Jsoup.parseBodyFragment(html);
        Elements tables = doc.select("table." + TABLE_BORDERED_CLASS);
        if (tables.isEmpty()) {
            return new ParsedLogHtml(html.trim(), List.of());
        }

        List<LogTable> out = new ArrayList<>();
        for (Element table : tables) {
            out.add(parseTable(table));
            table.remove();
        }

        String cleaned = doc.body() == null ? "" : doc.body().html();
        return new ParsedLogHtml(cleaned.trim(), out);
    }

    private static LogTable parseTable(Element table) {
        if (table == null) return new LogTable(List.of(), List.of());

        List<String> headers = new ArrayList<>();
        List<List<String>> rows = new ArrayList<>();

        Elements headerCells = new Elements();
        Elements dataRows;

        Element thead = table.selectFirst("thead");
        if (thead != null) {
            headerCells = thead.select("th,td");
            dataRows = table.select("tbody tr");
            if (dataRows.isEmpty()) {
                dataRows = table.select("tr");
                dataRows.removeIf(tr -> tr.parents().stream().anyMatch(p -> "thead".equals(p.tagName())));
            }
        } else {
            dataRows = table.select("tr");
            if (!dataRows.isEmpty() && !dataRows.get(0).select("th").isEmpty()) {
                headerCells = dataRows.get(0).select("th,td");
                dataRows = new Elements(dataRows.subList(1, dataRows.size()));
            }
        }

        if (!headerCells.isEmpty()) {
            headers = extractCellTexts(headerCells);
        }

        int maxCols = headers.size();
        for (Element row : dataRows) {
            Elements cells = row.select("th,td");
            if (cells.isEmpty()) continue;
            List<String> values = new ArrayList<>();
            for (Element cell : cells) {
                String text = cleanCellText(cell);
                int colspan = parseColspan(cell);
                for (int i = 0; i < colspan; i++) {
                    values.add(text);
                }
            }
            maxCols = Math.max(maxCols, values.size());
            rows.add(values);
        }

        if (headers.isEmpty()) {
            for (int i = 1; i <= maxCols; i++) {
                headers.add("Col " + i);
            }
        } else if (headers.size() < maxCols) {
            for (int i = headers.size() + 1; i <= maxCols; i++) {
                headers.add("Col " + i);
            }
        }

        for (List<String> row : rows) {
            while (row.size() < headers.size()) {
                row.add("");
            }
        }

        return new LogTable(headers, rows);
    }

    private static List<String> extractCellTexts(Elements cells) {
        List<String> out = new ArrayList<>();
        for (Element cell : cells) {
            out.add(cleanCellText(cell));
        }
        return out;
    }

    private static String cleanCellText(Element cell) {
        if (cell == null) return "";
        String text = cell.text();
        if (text == null) return "";
        return text.replaceAll("\\s+", " ").trim();
    }

    private static int parseColspan(Element cell) {
        if (cell == null) return 1;
        String raw = cell.attr("colspan");
        if (raw == null || raw.isBlank()) return 1;
        try {
            int v = Integer.parseInt(raw.trim());
            return Math.max(1, v);
        } catch (NumberFormatException ignored) {
            return 1;
        }
    }

    private static String scenarioDurationFromSteps(List<StepModel> steps) {
        Duration total = sumStepDurations(steps);
        if (total == null) return "—";
        return formatDuration(total);
    }

    private static String scenarioStartTimeFromSteps(List<Step> steps) {
        if (steps == null || steps.isEmpty()) return "";
        for (Step st : steps) {
            if (st == null || st.logs() == null) continue;
            for (Log lg : st.logs()) {
                String raw = lg == null ? "" : lg.detailsHtml();
                String time = extractLocalTime(raw);
                if (!time.isBlank()) {
                    return time;
                }
            }
        }
        return "";
    }

    private static Duration sumStepDurations(List<StepModel> steps) {
        if (steps == null || steps.isEmpty()) return null;
        long totalMillis = 0;
        boolean found = false;
        for (StepModel st : steps) {
            Duration d = sumLogDurations(st.logs());
            if (d != null) {
                found = true;
                totalMillis += d.toMillis();
            }
        }
        return found ? Duration.ofMillis(totalMillis) : null;
    }

    private static String extractLocalTime(String detailsHtml) {
        if (detailsHtml == null || detailsHtml.isBlank()) return "";
        Matcher m = LOG_LOCALTIME_VALUE_PATTERN.matcher(detailsHtml);
        if (m.find()) {
            return m.group(1).trim();
        }
        return "";
    }

    // We want Hebrew keyword display (like Extent in your screenshot) BUT strip English prefixes from name (When/And/etc).
    private static StepLabel splitStepLabelFromText(String rawText) {
        String text = rawText == null ? "" : rawText.trim();
        if (text.isBlank()) return new StepLabel("", "");

        for (KeywordPair kp : KEYWORD_PAIRS) {
            if (!kp.english().isBlank() && text.startsWith(kp.english() + " ")) {
                return new StepLabel(kp.hebrewOrEnglish(), text.substring(kp.english().length() + 1).trim());
            }
            if (!kp.hebrew().isBlank() && text.startsWith(kp.hebrew() + " ")) {
                return new StepLabel(kp.hebrewOrEnglish(), text.substring(kp.hebrew().length() + 1).trim());
            }
        }

        for (String k : List.of("Given", "When", "Then", "And", "But")) {
            if (text.startsWith(k + " ")) {
                return new StepLabel(k, text.substring(k.length() + 1).trim());
            }
        }

        return new StepLabel("", text);
    }

    private static final List<KeywordPair> KEYWORD_PAIRS = List.of(
            new KeywordPair("Given", "בהינתן"),
            new KeywordPair("When", "כאשר"),
            new KeywordPair("Then", "אז"),
            new KeywordPair("And", "וגם"),
            new KeywordPair("But", "אבל"),
            new KeywordPair("*", "צעד")
    );

    private record KeywordPair(String english, String hebrew) {
        String hebrewOrEnglish() {
            return !hebrew.isBlank() ? hebrew : english;
        }
    }

    // ---------------------- tree build ----------------------

    private void buildFeatureTree(List<FeatureModel> features) {
        this.featureIdToTreeNode.clear();
        Map<String, TreeNode<TreeItem>> groupNodes = new HashMap<>();
        Map<String, int[]> groupCounts = new HashMap<>();

        for (FeatureModel f : features) {
            if (f.groupPath() == null || f.groupPath().isEmpty()) continue;
            StringBuilder key = new StringBuilder();
            for (int i = 0; i < f.groupPath().size(); i++) {
                if (i > 0) key.append(" / ");
                key.append(f.groupPath().get(i));
                addGroupCount(groupCounts, key.toString(), f.status());
            }
        }

        // IMPORTANT:
        // If your XHTML uses <p:treeNode type="GROUP"> and <p:treeNode type="FEATURE">,
        // then nodes MUST be created with matching "type" strings or they will render blank.
        this.featureTreeRoot = new DefaultTreeNode<>("ROOT",
                new TreeItem(TreeType.ROOT, "ROOT", null, null, List.of(), 0, 0, 0, 0),
                null
        );

        for (FeatureModel f : features) {
            TreeNode<TreeItem> parent = featureTreeRoot;

            StringBuilder key = new StringBuilder();
            for (int i = 0; i < f.groupPath().size(); i++) {
                if (i > 0) key.append(" / ");
                key.append(f.groupPath().get(i));
                parent = ensureGroupNode(parent, f.groupPath().get(i), key.toString(), groupNodes, groupCounts);
            }

            TreeItem leaf = new TreeItem(TreeType.FEATURE, f.title(), f.status(), f.id(), f.tags(), 0, 0, 0, 0);
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
            Map<String, int[]> groupCounts
    ) {
        TreeNode<TreeItem> existing = groupNodes.get(pathKey);
        if (existing != null) {
            return existing;
        }
        int[] counts = groupCounts.getOrDefault(pathKey, new int[4]);
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
                        counts[3]
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

    public MenuModel getMenubarModel() { return menubarModel; }

    public String getRunMenuLabel() { return runMenuLabel; }

    public String runLabel(String runFolder) {
        if (runFolder == null) return "";
        return runLabelsByFolder.getOrDefault(runFolder, runFolder);
    }

    private void buildMenubarModel() {
        DefaultMenuModel model = new DefaultMenuModel();
        runMenuLabel = (run == null || run.isBlank()) ? "בחירת תאריך" : runLabel(run);

        if (recentRuns == null || recentRuns.isEmpty()) {
            DefaultMenuItem empty = DefaultMenuItem.builder()
                    .value("ללא היסטוריה")
                    .disabled(true)
                    .build();
            model.addElement(empty);
        } else {
            for (RunOption r : recentRuns) {
                DefaultMenuItem item = DefaultMenuItem.builder()
                        .value(r.label())
                        .outcome("details")
                        .title(r.tooltip())
                        .styleClass(r.current() ? "menu-current" : null)
                        .build();
                item.setParam("filter", filter);
                item.setParam("item", this.item);
                item.setParam("run", r.runFolder());
                model.addElement(item);
            }
        }

        this.menubarModel = model;
    }

    public List<RunStatus> getScenarioHistory(ScenarioModel sc) {
        if (selectedFeature == null || sc == null) return List.of();
        String key = scenarioKeyFor(selectedFeature, sc);
        List<RunStatus> history = scenarioHistoryByKey.get(key);
        return history == null ? List.of() : history;
    }

    public List<RunStatus> scenarioHistory(ScenarioModel sc) {
        return getScenarioHistory(sc);
    }

    public List<RunStatus> featureHistory(FeatureModel feature) {
        if (feature == null || recentRuns == null || recentRuns.isEmpty()) return List.of();

        Map<String, List<String>> statusesByRun = new LinkedHashMap<>();
        for (RunOption r : recentRuns) {
            statusesByRun.put(r.runFolder(), new ArrayList<>());
        }

        if (feature.scenarios() != null) {
            for (ScenarioModel sc : feature.scenarios()) {
                String key = scenarioKeyFor(feature, sc);
                List<RunStatus> history = scenarioHistoryByKey.get(key);
                if (history == null) continue;
                for (RunStatus rs : history) {
                    List<String> bucket = statusesByRun.get(rs.runFolder());
                    if (bucket != null) {
                        bucket.add(rs.status());
                    }
                }
            }
        }

        List<RunStatus> out = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : statusesByRun.entrySet()) {
            String agg = aggregateStatus(entry.getValue());
            if (!agg.isBlank()) {
                out.add(new RunStatus(entry.getKey(), agg));
            }
        }
        return out;
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

    private static String aggregateStatus(List<String> statuses) {
        if (statuses == null || statuses.isEmpty()) return "";
        boolean sawPass = false;
        boolean sawSkip = false;
        boolean sawKnownBug = false;
        boolean sawFail = false;

        for (String status : statuses) {
            String norm = RunHistoryAnalyzer.normalizeStatus(status);
            switch (norm) {
                case "FAIL" -> sawFail = true;
                case "KNOWNBUG" -> sawKnownBug = true;
                case "SKIP" -> sawSkip = true;
                case "PASS" -> sawPass = true;
                default -> { }
            }
        }

        if (sawFail) return "FAIL";
        if (sawKnownBug) return "KNOWNBUG";
        if (sawSkip) return "SKIP";
        if (sawPass) return "PASS";
        return "UNKNOWN";
    }

    // ---------------------- time helpers ----------------------

    private static LocalDateTime parseExtent(String raw) {
        if (raw == null || raw.isBlank()) return null;
        String s = raw.replace('\u202F', ' ')  // narrow no-break space
                .replace('\u00A0', ' ')       // no-break space
                .replaceAll("\\s+", " ")
                .trim();
        for (DateTimeFormatter fmt : EXTENT_TIME_FMTS) {
            try {
                return LocalDateTime.parse(s, fmt);
            } catch (Exception ignored) {
                // try next
            }
        }
        return null;
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
            int skipCount
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

    public record FeatureModel(
            String id,
            List<String> groupPath,
            String title,
            String status,
            String durationText,
            List<String> tags,
            String descriptionHtml,
            List<ScenarioModel> scenarios
    ) implements Serializable {}

    public record ScenarioModel(
            String id,
            String name,
            String status,
            String startTimeText,
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
            long durationMillis,
            List<LogTable> tables
    ) implements Serializable {}

    private record StepLabel(String keyword, String text) {}

    private record ParsedLogHtml(String html, List<LogTable> tables) {}

    public record LogTable(
            List<String> headers,
            List<List<String>> rows
    ) implements Serializable {}
}
