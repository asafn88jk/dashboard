package com.leumit.dashboard.run;

import com.leumit.dashboard.model.ExtentSummary;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class SparkHtmlReportParser {

    private static final Pattern STATUS_GROUP_BLOCK =
            Pattern.compile("var\\s+statusGroup\\s*=\\s*\\{(.*?)\\};", Pattern.DOTALL);
    private static final Pattern STATUS_GROUP_ENTRY =
            Pattern.compile("(\\w+)\\s*:\\s*(\\d+)");
    private static final Pattern DASHBOARD_STARTED_PATTERN = Pattern.compile(
            "<p[^>]*>\\s*Started\\s*</p>\\s*<h3[^>]*>([^<]+)</h3>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final Pattern DASHBOARD_ENDED_PATTERN = Pattern.compile(
            "<p[^>]*>\\s*Ended\\s*</p>\\s*<h3[^>]*>([^<]+)</h3>",
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );
    private static final List<DateTimeFormatter> REPORT_TIME_FMTS = List.of(
            DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("MM.dd.yyyy h:mm:ss a", Locale.ENGLISH),
            DateTimeFormatter.ofPattern("M.d.yyyy h:mm:ss a", Locale.ENGLISH)
    );

    private SparkHtmlReportParser() {}

    public record ParsedReport(ExtentSummary summary, List<Feature> features) {}

    public record Feature(
            String name,
            List<String> path,
            String status,
            List<String> tags,
            String descriptionHtml,
            String startTime,
            String endTime,
            List<Scenario> scenarios
    ) {}

    public record Scenario(
            String name,
            String status,
            List<Step> steps
    ) {}

    public record Step(
            String text,
            String status,
            List<Log> logs
    ) {}

    public record Log(
            String detailsHtml,
            String mediaPath
    ) {}

    public static Optional<Path> findReportHtml(Path runDir) {
        if (runDir == null || !Files.isDirectory(runDir)) return Optional.empty();
        try (Stream<Path> s = Files.list(runDir)) {
            return s.filter(Files::isRegularFile)
                    .filter(SparkHtmlReportParser::isHtmlFile)
                    .max(Comparator.comparingLong(SparkHtmlReportParser::safeSize));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    public static Path requireReportHtml(Path runDir) {
        return findReportHtml(runDir)
                .orElseThrow(() -> new IllegalArgumentException("Missing Spark HTML report in: " + runDir));
    }

    public static ExtentSummary parseSummary(Path reportHtml) throws IOException {
        String html = Files.readString(reportHtml);
        ExtentSummary.Totals totals = totalsFromStatusGroup(html);
        String started = findDashboardValueFast(html, DASHBOARD_STARTED_PATTERN);
        String ended = findDashboardValueFast(html, DASHBOARD_ENDED_PATTERN);

        if (totals != null) {
            ExtentSummary.Run run = new ExtentSummary.Run(started, ended);
            return new ExtentSummary(run, totals, List.of());
        }

        Document doc = Jsoup.parse(html);
        return parseSummary(doc, html, null);
    }

    public static List<Feature> parseFeatures(Path reportHtml) throws IOException {
        String html = Files.readString(reportHtml);
        Document doc = Jsoup.parse(html);
        return parseFeatures(doc);
    }

    public static List<Feature> parseFeaturesNoLogs(Path reportHtml) throws IOException {
        String html = Files.readString(reportHtml);
        Document doc = Jsoup.parse(html);
        return parseFeatures(doc, false);
    }

    public static ParsedReport parseReport(Path reportHtml) throws IOException {
        String html = Files.readString(reportHtml);
        Document doc = Jsoup.parse(html);
        List<Feature> features = parseFeatures(doc);
        ExtentSummary summary = parseSummary(doc, html, features);
        return new ParsedReport(summary, features);
    }

    public static Optional<LocalDateTime> parseReportDateTime(String raw) {
        if (raw == null || raw.isBlank()) return Optional.empty();
        String s = raw.replace('\u202F', ' ')
                .replace('\u00A0', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        for (DateTimeFormatter fmt : REPORT_TIME_FMTS) {
            try {
                return Optional.of(LocalDateTime.parse(s, fmt));
            } catch (Exception ignored) {
                // try next
            }
        }
        return Optional.empty();
    }

    public static Optional<LocalDate> resolveReportDate(ExtentSummary summary, Path reportHtml) {
        if (summary != null && summary.run() != null) {
            Optional<LocalDateTime> start = parseReportDateTime(summary.run().startTime());
            if (start.isPresent()) {
                return Optional.of(start.get().toLocalDate());
            }

            Optional<LocalDateTime> end = parseReportDateTime(summary.run().endTime());
            if (end.isPresent()) {
                return Optional.of(end.get().toLocalDate());
            }
        }

        if (reportHtml != null) {
            try {
                FileTime ts = Files.getLastModifiedTime(reportHtml);
                Instant instant = ts.toInstant();
                LocalDate date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault()).toLocalDate();
                return Optional.of(date);
            } catch (IOException ignored) {
                // fall through
            }
        }

        return Optional.empty();
    }

    public static boolean isBeforeCutoff(ExtentSummary summary, Path reportHtml, LocalDate cutoff) {
        if (cutoff == null) return false;
        Optional<LocalDate> reportDate = resolveReportDate(summary, reportHtml);
        return reportDate.isPresent() && reportDate.get().isBefore(cutoff);
    }

    private static ExtentSummary parseSummary(Document doc, String html, List<Feature> features) {
        String started = findDashboardValue(doc, "Started");
        String ended = findDashboardValue(doc, "Ended");

        ExtentSummary.Totals totals = totalsFromStatusGroup(html);
        if (totals == null) {
            totals = totalsFromFeatures(features != null ? features : parseFeatures(doc));
        }

        ExtentSummary.Run run = new ExtentSummary.Run(started, ended);
        return new ExtentSummary(run, totals, List.of());
    }

    private static List<Feature> parseFeatures(Document doc) {
        return parseFeatures(doc, true);
    }

    private static List<Feature> parseFeatures(Document doc, boolean includeLogs) {
        List<Feature> features = new ArrayList<>();

        Element testView = doc.selectFirst("div.test-wrapper.view.test-view");
        Iterable<Element> featureEls = (testView != null)
                ? testView.select("ul.test-list-item > li.test-item")
                : doc.select("ul.test-list-item > li.test-item");

        for (Element featureEl : featureEls) {
            String name = textOf(featureEl.selectFirst("div.test-detail > p.name"));
            if (name.isBlank()) {
                name = textOf(featureEl.selectFirst("div.detail-head h5.test-status"));
            }

            String status = normalizeStatus(featureEl.attr("status"));
            List<String> tags = parseTags(featureEl.attr("tag"));
            if (tags.isEmpty()) {
                tags = parseTagsFromBadges(featureEl);
            }

            Element detailHead = featureEl.selectFirst("div.test-contents .detail-head");
            String start = textOf(detailHead == null ? null : detailHead.selectFirst("span.badge-success"));
            String end = textOf(detailHead == null ? null : detailHead.selectFirst("span.badge-danger"));
            String descriptionHtml = extractDescriptionHtml(detailHead, featureEl);

            List<String> path = parseArrowPath(name);
            List<Scenario> scenarios = parseScenarios(featureEl, includeLogs);

            features.add(new Feature(name, path, status, tags, descriptionHtml, start, end, scenarios));
        }

        return features;
    }

    private static List<Scenario> parseScenarios(Element featureEl, boolean includeLogs) {
        Element accordion = featureEl.selectFirst("div.accordion");
        if (accordion == null) return List.of();

        List<Scenario> scenarios = new ArrayList<>();
        for (Element card : accordion.select("> div.card")) {
            Element outline = card.selectFirst("> div.scenario_outline");
            if (outline != null) {
                List<Scenario> outlineScenarios = parseScenarioOutline(outline, includeLogs);
                if (outlineScenarios.isEmpty()) {
                    Scenario sc = parseScenarioCard(card, includeLogs);
                    if (sc != null) scenarios.add(sc);
                } else {
                    scenarios.addAll(outlineScenarios);
                }
                continue;
            }

            Scenario sc = parseScenarioCard(card, includeLogs);
            if (sc != null) scenarios.add(sc);
        }
        return scenarios;
    }

    private static List<Scenario> parseScenarioOutline(Element outline, boolean includeLogs) {
        List<Scenario> scenarios = new ArrayList<>();
        for (Element body : outline.select("div.card-body.l1")) {
            Element node = body.selectFirst("div.card-header .node");
            if (node == null) continue;

            String name = nodeName(node);
            String status = statusFromNode(node);
            Element stepsContainer = firstNonNull(
                    body.selectFirst("div.card-body.mt-3"),
                    body.selectFirst("div.card-body")
            );

            scenarios.add(new Scenario(name, status, parseSteps(stepsContainer, includeLogs)));
        }
        return scenarios;
    }

    private static Scenario parseScenarioCard(Element card, boolean includeLogs) {
        Element node = card.selectFirst("> div.card-header .node");
        if (node == null) return null;

        String name = nodeName(node);
        String status = statusFromNode(node);

        Element stepsContainer = firstNonNull(
                card.selectFirst("> div.collapse > div.card-body"),
                card.selectFirst("> div > div.card-body"),
                card.selectFirst("> div.card-body"),
                card.selectFirst("div.card-body")
        );

        return new Scenario(name, status, parseSteps(stepsContainer, includeLogs));
    }

    private static List<Step> parseSteps(Element container, boolean includeLogs) {
        if (container == null) return List.of();
        List<Step> steps = new ArrayList<>();

        for (Element child : container.children()) {
            if (!child.hasClass("step")) continue;

            String status = statusFromClass(child);
            String text = textOf(child.selectFirst("> span"));
            if (text.isBlank()) {
                text = child.ownText().trim();
            }

            boolean afterStep = isAfterStep(child.attr("title"));

            if (afterStep) {
                if (includeLogs && !steps.isEmpty()) {
                    List<Log> logs = parseStepLogs(child);
                    Step prev = steps.remove(steps.size() - 1);
                    List<Log> merged = new ArrayList<>(prev.logs());
                    merged.addAll(logs);
                    steps.add(new Step(prev.text(), prev.status(), merged));
                }
                continue;
            }

            List<Log> logs = includeLogs ? parseStepLogs(child) : List.of();
            steps.add(new Step(text, status, logs));
        }

        return steps;
    }

    private static List<Log> parseStepLogs(Element stepEl) {
        List<Log> logs = new ArrayList<>();

        for (Element child : stepEl.children()) {
            if (!"div".equalsIgnoreCase(child.tagName())) continue;

            Element cleaned = child.clone();
            String mediaPath = null;
            Element img = cleaned.selectFirst("img");
            if (img != null) {
                String p = img.hasAttr("data-featherlight") ? img.attr("data-featherlight") : img.attr("src");
                if (isLocalMediaPath(p)) {
                    mediaPath = p.trim();
                    cleaned.select("img").remove();
                }
            }

            String html = cleaned.html().trim();
            if (html.isBlank() && (mediaPath == null || mediaPath.isBlank())) {
                continue;
            }
            logs.add(new Log(html, mediaPath));
        }

        return logs;
    }

    private static ExtentSummary.Totals totalsFromStatusGroup(String html) {
        if (html == null || html.isBlank()) return null;
        Matcher m = STATUS_GROUP_BLOCK.matcher(html);
        if (!m.find()) return null;

        String body = m.group(1);
        Matcher kv = STATUS_GROUP_ENTRY.matcher(body);
        Map<String, Integer> counts = new HashMap<>();
        while (kv.find()) {
            counts.put(kv.group(1), Integer.parseInt(kv.group(2)));
        }

        Integer pass = counts.get("passParent");
        Integer fail = counts.get("failParent");
        Integer warning = counts.get("warningParent");
        Integer skip = counts.get("skipParent");
        if (pass == null && fail == null && warning == null && skip == null) return null;

        int p = pass == null ? 0 : pass;
        int f = fail == null ? 0 : fail;
        int w = warning == null ? 0 : warning;
        int s = skip == null ? 0 : skip;
        int total = p + f + w + s;
        double passRate = total > 0 ? (p * 1.0) / total : 0.0;

        return new ExtentSummary.Totals(p, f, w, s, total, passRate);
    }

    private static ExtentSummary.Totals totalsFromFeatures(List<Feature> features) {
        if (features == null || features.isEmpty()) {
            return new ExtentSummary.Totals(0, 0, 0, 0, 0, 0.0);
        }

        int pass = 0;
        int fail = 0;
        int knownBug = 0;
        int skip = 0;

        for (Feature f : features) {
            String st = normalizeStatus(f.status());
            switch (st) {
                case "PASS" -> pass++;
                case "FAIL" -> fail++;
                case "SKIP" -> skip++;
                case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> knownBug++;
                default -> { }
            }
        }

        int total = pass + fail + knownBug + skip;
        double passRate = total > 0 ? (pass * 1.0) / total : 0.0;
        return new ExtentSummary.Totals(pass, fail, knownBug, skip, total, passRate);
    }

    private static String findDashboardValue(Document doc, String label) {
        if (doc == null || label == null) return "";
        Element dashboard = doc.selectFirst("div.container-fluid.p-4.view.dashboard-view");
        if (dashboard == null) return "";

        for (Element body : dashboard.select("div.card-body")) {
            Element p = body.selectFirst("p");
            if (p == null) continue;
            if (!label.equalsIgnoreCase(p.text().trim())) continue;
            Element h3 = body.selectFirst("h3");
            return textOf(h3);
        }
        return "";
    }

    private static String findDashboardValueFast(String html, Pattern pattern) {
        if (html == null || html.isBlank() || pattern == null) return "";
        Matcher m = pattern.matcher(html);
        if (m.find()) {
            return m.group(1).trim();
        }
        return "";
    }

    private static String nodeName(Element node) {
        if (node == null) return "";
        String own = node.ownText();
        if (own != null && !own.isBlank()) return own.trim();
        return node.text().trim();
    }

    private static String statusFromNode(Element node) {
        if (node == null) return "";
        Element badge = node.selectFirst("span.badge");
        if (badge != null) {
            String txt = badge.text();
            if (txt != null && !txt.isBlank()) return normalizeStatus(txt);
        }
        return normalizeStatus(node.attr("status"));
    }

    private static String statusFromClass(Element el) {
        if (el == null) return "";
        String cls = el.className().toLowerCase(Locale.ROOT);
        if (cls.contains("pass-bg")) return "PASS";
        if (cls.contains("fail-bg")) return "FAIL";
        if (cls.contains("skip-bg")) return "SKIP";
        if (cls.contains("warning-bg")) return "WARNING";
        if (cls.contains("info-bg")) return "INFO";
        return "";
    }

    private static boolean isAfterStep(String title) {
        return "AFTER_STEP".equalsIgnoreCase(String.valueOf(title).trim());
    }

    private static String normalizeStatus(String raw) {
        if (raw == null) return "";
        String s = raw.trim().toUpperCase(Locale.ROOT);
        return switch (s) {
            case "PASS" -> "PASS";
            case "FAIL" -> "FAIL";
            case "SKIP" -> "SKIP";
            case "WARNING", "KNOWNBUG", "KNOWN_BUG", "KNOWN BUG" -> "WARNING";
            case "INFO" -> "INFO";
            default -> s;
        };
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

    private static List<String> parseTags(String raw) {
        if (raw == null || raw.isBlank()) return List.of();
        String[] parts = raw.trim().split("\\s+");
        List<String> out = new ArrayList<>();
        for (String p : parts) {
            String t = p.trim();
            if (!t.isBlank()) out.add(t);
        }
        return out;
    }

    private static List<String> parseTagsFromBadges(Element featureEl) {
        if (featureEl == null) return List.of();
        List<String> tags = new ArrayList<>();
        for (Element badge : featureEl.select("div.detail-head span.badge-pill")) {
            String t = badge.text().trim();
            if (!t.isBlank()) tags.add(t);
        }
        return tags;
    }

    private static boolean isHtmlFile(Path p) {
        String name = p.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".html") || name.endsWith(".htm");
    }

    private static long safeSize(Path p) {
        try {
            return Files.size(p);
        } catch (IOException e) {
            return -1L;
        }
    }

    private static boolean isLocalMediaPath(String p) {
        if (p == null || p.isBlank()) return false;
        String s = p.trim().toLowerCase(Locale.ROOT);
        return !(s.startsWith("http://")
                || s.startsWith("https://")
                || s.startsWith("data:")
                || s.startsWith("blob:"));
    }

    private static String textOf(Element el) {
        return el == null ? "" : el.text().trim();
    }

    private static String htmlOf(Element el) {
        return el == null ? "" : el.html().trim();
    }

    private static String extractDescriptionHtml(Element detailHead, Element featureEl) {
        String description = cleanDescriptionHtml(htmlOf(
                detailHead == null ? null : detailHead.selectFirst("div.m-t-10.m-l-5")
        ));

        if (description.isBlank()) {
            description = cleanDescriptionHtml(htmlOf(featureEl.selectFirst(
                    "div.test-detail > p.desc, div.test-detail > p.test-desc, div.test-detail > p.description"
            )));
        }

        if (description.isBlank()) {
            description = cleanDescriptionHtml(htmlOf(
                    detailHead == null ? null : detailHead.selectFirst(".test-desc, .test-description, p.desc, p.description")
            ));
        }

        return description;
    }

    private static String cleanDescriptionHtml(String html) {
        if (html == null || html.isBlank()) return "";
        String cleaned = html.replace('\u00A0', ' ').trim();
        cleaned = cleaned.replaceAll("(?i)(<br\\s*/?>\\s*)+$", "");
        return cleaned.trim();
    }

    private static <T> T firstNonNull(T a, T b) {
        return a != null ? a : b;
    }
}
