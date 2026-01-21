package com.leumit.dashboard.view;

import com.leumit.dashboard.config.DashboardFiltersProperties;
import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.repo.RunPicker;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import software.xdev.chartjs.model.charts.DoughnutChart;
import software.xdev.chartjs.model.color.RGBAColor;
import software.xdev.chartjs.model.data.DoughnutData;
import software.xdev.chartjs.model.dataset.DoughnutDataset;
import software.xdev.chartjs.model.options.DoughnutOptions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;

@Component("dashboardView")
@Scope("view")
@Slf4j
public class DashboardView implements Serializable {

  private final DashboardFiltersProperties props;
  private final RunPicker runPicker;
  private final int autoRefreshSeconds;

  private List<String> filters;
  private String activeFilter;
  private List<DashboardCard> cards;
  private String activePage;

  private LocalDate graphStartDate;
  private LocalDate graphEndDate;
  private Set<String> graphSelectedSetIds = new LinkedHashSet<>();
  private Set<String> graphSelectedStatuses = new LinkedHashSet<>();
  private List<GraphGroup> graphGroups = List.of();
  private Map<String, GraphSet> graphSetIndex = new HashMap<>();
  private String graphChartJson;
  private boolean graphDirty = true;

  public DashboardView(
          DashboardFiltersProperties props,
          RunPicker runPicker,
          @Value("${dashboard.autoRefreshSeconds:60}") int autoRefreshSeconds
  ) {
    this.props = props;
    this.runPicker = runPicker;
    this.autoRefreshSeconds = Math.max(0, autoRefreshSeconds);
  }

  private static final List<DateTimeFormatter> EXTENT_TIME_FMTS = List.of(
          DateTimeFormatter.ofPattern("MMM d, yyyy, h:mm:ss a", Locale.ENGLISH),
          DateTimeFormatter.ofPattern("MMM d, yyyy h:mm:ss a", Locale.ENGLISH),
          DateTimeFormatter.ofPattern("MM.dd.yyyy h:mm:ss a", Locale.ENGLISH),
          DateTimeFormatter.ofPattern("M.d.yyyy h:mm:ss a", Locale.ENGLISH)
  );
  private static final DateTimeFormatter GRAPH_LABEL_FMT =
          DateTimeFormatter.ofPattern("dd.MM HH:mm", Locale.ENGLISH);
  private static final DateTimeFormatter GRAPH_RANGE_FMT =
          DateTimeFormatter.ofPattern("dd.MM.yyyy", Locale.ENGLISH);
  private static final ObjectMapper GRAPH_MAPPER = new ObjectMapper();
  private static final List<StatusOption> GRAPH_STATUS_OPTIONS = List.of(
          new StatusOption("FAIL", "נכשל"),
          new StatusOption("SKIP", "דולג"),
          new StatusOption("KNOWN_BUG", "באג ידוע"),
          new StatusOption("PASS", "עבר")
  );
  private static final List<String> GRAPH_COLORS = List.of(
          "#2563eb",
          "#f97316",
          "#6b7280",
          "#0ea5e9",
          "#16a34a",
          "#a855f7",
          "#f43f5e",
          "#0891b2"
  );
  private static LocalDateTime parseExtentTime(String raw) {
    if (raw == null || raw.isBlank()) return null;

    // Extent uses "Jan 13, 2026, 12:12:53 PM" (note the narrow no-break space before PM)
    String s = raw
            .replace('\u202F', ' ') // narrow no-break space
            .replace('\u00A0', ' ') // non-breaking space
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

  private static String formatDuration(Duration d) {
    long seconds = Math.max(0, d.getSeconds());
    long h = seconds / 3600;
    long m = (seconds % 3600) / 60;
    long s = seconds % 60;

    if (h > 0) {
      // H:MM:SS
      return h + ":" + two(m) + ":" + two(s);
    }
    // M:SS
    return m + ":" + two(s);
  }

  private static String two(long n) {
    return (n < 10) ? ("0" + n) : Long.toString(n);
  }


  @PostConstruct
  public void init() {
    filters = props.getFilters().stream().map(DashboardFiltersProperties.Filter::getName).toList();
    activeFilter = filters.isEmpty() ? "" : filters.get(0);
    cards = loadCardsFor(activeFilter);
    activePage = "dashboard";
    initGraphConfig();
  }

  public void setActiveFilter(String activeFilter) {
    this.activeFilter = activeFilter;
    this.cards = loadCardsFor(activeFilter);
  }

  public void refreshCards() {
    if (activeFilter == null || activeFilter.isBlank()) return;
    this.cards = loadCardsFor(activeFilter);
  }

  private List<DashboardCard> loadCardsFor(String filterName) {
    DashboardFiltersProperties.Filter f = props.getFilters().stream()
            .filter(x -> Objects.equals(x.getName(), filterName))
            .findFirst()
            .orElse(null);

    if (f == null) return List.of();

    long startNs = System.nanoTime();
    List<DashboardCard> out = new ArrayList<>();

    for (DashboardFiltersProperties.Item item : f.getItems()) {
      long itemStartNs = System.nanoTime();
      try {
        Path base = Path.of(item.getBaseDir());
        Pattern p = Pattern.compile(item.getDirNameRegex());

        Optional<RunPicker.PickedRun> pickedOpt = runPicker.pickLatestFast(base, p);
        if (pickedOpt.isEmpty()) {
          long itemMs = (System.nanoTime() - itemStartNs) / 1_000_000;
          log.info("DashboardView.loadCards item={} runs=0 totalMs={}", item.getTitle(), itemMs);
          continue;
        }

        RunPicker.PickedRun picked = pickedOpt.get();
          var totals = picked.summary().totals();

          int total = Math.max(1, totals.total());
          int passPct = (int) Math.round((totals.pass() * 100.0) / total);

          String json = doughnutJson(totals.pass(), totals.fail(), totals.knownBug(), totals.skip());

          LocalDateTime startedAt = toLocalDateTime(picked.lastModifiedMillis());
          String durationText = "—";

          var run = picked.summary().run();
          LocalDateTime start = (run != null) ? parseExtentTime(run.startTime()) : null;
          LocalDateTime end   = (run != null) ? parseExtentTime(run.endTime())   : null;

          if (start != null) {
            startedAt = start;
          }
          if (start != null && end != null) {
            Duration dur = Duration.between(start, end);
            if (!dur.isNegative() && !dur.isZero()) {
              durationText = formatDuration(dur);
            }
          }

          String runFolder = picked.runDir().getFileName().toString();

          out.add(new DashboardCard(
                  item.getTitle(),
                  passPct,
                  durationText,
                  startedAt,
                  json,
                  filterName,
                  item.getTitle(),
                  runFolder
          ));

          long itemMs = (System.nanoTime() - itemStartNs) / 1_000_000;
          log.info("DashboardView.loadCards item={} runs=1 totalMs={}", item.getTitle(), itemMs);
      } catch (Exception ignored) {
        // If an item is misconfigured (missing dir, regex, json), just skip it
        long itemMs = (System.nanoTime() - itemStartNs) / 1_000_000;
        log.info("DashboardView.loadCards item={} error totalMs={}", item.getTitle(), itemMs);
      }
    }

    long totalMs = (System.nanoTime() - startNs) / 1_000_000;
    log.info("DashboardView.loadCards filter={} items={} cards={} totalMs={}",
            filterName, f.getItems().size(), out.size(), totalMs);
    return out;
  }

  private static LocalDateTime toLocalDateTime(long millis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault());
  }

  private String doughnutJson(int pass, int fail, int knownBug, int skip) {
    DoughnutDataset dataset = new DoughnutDataset()
            .setData(
                    BigDecimal.valueOf(pass),
                    BigDecimal.valueOf(fail),
                    BigDecimal.valueOf(knownBug),
                    BigDecimal.valueOf(skip)
            )
            .addBackgroundColors(
                    new RGBAColor(34, 197, 94),    // pass
                    new RGBAColor(239, 68, 68),    // fail
                    new RGBAColor(245, 158, 11),   // known bug
                    new RGBAColor(148, 163, 184)   // skip (soft gray)
            );

    DoughnutData data = new DoughnutData()
            .addDataset(dataset)
            .setLabels("עבר בהצלחה", "נכשל", "באג ידוע", "דולג");

    DoughnutOptions options = new DoughnutOptions()
            .setMaintainAspectRatio(Boolean.FALSE)
            .setResponsive(Boolean.TRUE);

    return ChartJsJsonTweaks.applyDashboardTweaks(
            new DoughnutChart().setData(data).setOptions(options).toJson()
    );
  }

  // ---------------------- graph view ----------------------

  public String getActivePage() { return activePage; }

  public void setActivePage(String activePage) {
    if (activePage == null || activePage.isBlank()) return;
    this.activePage = activePage;
  }

  public List<GraphGroup> getGraphGroups() { return graphGroups; }

  public Set<String> getGraphSelectedSetIds() { return graphSelectedSetIds; }

  public void setGraphSelectedSetIds(Set<String> graphSelectedSetIds) {
    this.graphSelectedSetIds = graphSelectedSetIds == null
            ? new LinkedHashSet<>()
            : new LinkedHashSet<>(graphSelectedSetIds);
    graphDirty = true;
  }

  public Set<String> getGraphSelectedStatuses() { return graphSelectedStatuses; }

  public void setGraphSelectedStatuses(Set<String> graphSelectedStatuses) {
    this.graphSelectedStatuses = graphSelectedStatuses == null
            ? new LinkedHashSet<>()
            : new LinkedHashSet<>(graphSelectedStatuses);
    graphDirty = true;
  }

  public List<StatusOption> getGraphStatusOptions() { return GRAPH_STATUS_OPTIONS; }

  public LocalDate getGraphStartDate() { return graphStartDate; }

  public void setGraphStartDate(LocalDate graphStartDate) {
    this.graphStartDate = graphStartDate;
    graphDirty = true;
  }

  public LocalDate getGraphEndDate() { return graphEndDate; }

  public void setGraphEndDate(LocalDate graphEndDate) {
    this.graphEndDate = graphEndDate;
    graphDirty = true;
  }

  public String getGraphChartJson() {
    if (graphDirty || graphChartJson == null) {
      graphChartJson = buildGraphChartJson();
      graphDirty = false;
    }
    return graphChartJson;
  }

  public String getGraphTitle() {
    LocalDate start = graphStartDate == null ? LocalDate.now().minusWeeks(2) : graphStartDate;
    LocalDate end = graphEndDate == null ? LocalDate.now() : graphEndDate;
    if (end.isBefore(start)) {
      LocalDate tmp = start;
      start = end;
      end = tmp;
    }
    return "סטטיסטיקה לטווח תאריכים " + start.format(GRAPH_RANGE_FMT) + " - " + end.format(GRAPH_RANGE_FMT);
  }

  public void selectAllGroup(String groupId) {
    GraphGroup group = graphGroups.stream()
            .filter(g -> Objects.equals(g.id(), groupId))
            .findFirst()
            .orElse(null);
    if (group == null) return;
    for (GraphSet set : group.items()) {
      graphSelectedSetIds.add(set.id());
    }
    graphDirty = true;
  }

  public void selectNoneGroup(String groupId) {
    GraphGroup group = graphGroups.stream()
            .filter(g -> Objects.equals(g.id(), groupId))
            .findFirst()
            .orElse(null);
    if (group == null) return;
    for (GraphSet set : group.items()) {
      graphSelectedSetIds.remove(set.id());
    }
    graphDirty = true;
  }

  private void initGraphConfig() {
    List<GraphGroup> groups = new ArrayList<>();
    Map<String, GraphSet> index = new LinkedHashMap<>();

    for (DashboardFiltersProperties.Filter f : props.getFilters()) {
      List<GraphSet> items = new ArrayList<>();
      for (DashboardFiltersProperties.Item item : f.getItems()) {
        String id = f.getName() + "::" + item.getTitle();
        GraphSet set = new GraphSet(id, item.getTitle(), f.getName(), item.getBaseDir(), item.getDirNameRegex());
        items.add(set);
        index.put(id, set);
      }
      groups.add(new GraphGroup(f.getName(), f.getName(), items));
    }

    graphGroups = groups;
    graphSetIndex = index;

    graphSelectedSetIds = new LinkedHashSet<>(index.keySet());
    graphSelectedStatuses = new LinkedHashSet<>(List.of("FAIL", "SKIP"));
    graphStartDate = LocalDate.now().minusWeeks(2);
    graphEndDate = LocalDate.now();
    graphDirty = true;
  }

  private String buildGraphChartJson() {
    List<GraphSet> selectedSets = new ArrayList<>();
    for (String id : graphSelectedSetIds) {
      GraphSet set = graphSetIndex.get(id);
      if (set != null) selectedSets.add(set);
    }

    if (selectedSets.isEmpty()) {
      return emptyLineChartJson(getGraphTitle());
    }

    LocalDate start = graphStartDate == null ? LocalDate.now().minusWeeks(2) : graphStartDate;
    LocalDate end = graphEndDate == null ? LocalDate.now() : graphEndDate;
    if (end.isBefore(start)) {
      LocalDate tmp = start;
      start = end;
      end = tmp;
    }

    ZoneId zone = ZoneId.systemDefault();
    Instant from = start.atStartOfDay(zone).toInstant();
    Instant to = end.atTime(LocalTime.MAX).atZone(zone).toInstant();

    SortedSet<LocalDateTime> allTimes = new TreeSet<>();
    Map<String, Map<LocalDateTime, GraphPoint>> valuesBySet = new LinkedHashMap<>();

    for (GraphSet set : selectedSets) {
      Map<LocalDateTime, GraphPoint> values = new HashMap<>();
      try {
        Path base = Path.of(set.baseDir());
        Pattern p = Pattern.compile(set.dirNameRegex());
        List<RunPicker.PickedRun> runs = runPicker.pickRunsBetween(base, p, from, to);
        for (RunPicker.PickedRun run : runs) {
          LocalDateTime runStart = resolveRunStart(run.summary(), run.lastModifiedMillis());
          if (runStart == null) continue;
          if (runStart.isBefore(start.atStartOfDay()) || runStart.isAfter(end.atTime(LocalTime.MAX))) {
            continue;
          }
          GraphPoint point = toGraphPoint(run.summary(), graphSelectedStatuses);
          if (point == null) continue;
          values.put(runStart, point);
          allTimes.add(runStart);
        }
      } catch (Exception ignored) {
        // skip bad item
      }
      valuesBySet.put(set.id(), values);
    }

    List<LocalDateTime> times = new ArrayList<>(allTimes);
    List<String> labels = new ArrayList<>();
    for (LocalDateTime t : times) {
      labels.add(t.format(GRAPH_LABEL_FMT));
    }

    ObjectNode root = GRAPH_MAPPER.createObjectNode();
    root.put("type", "line");

    ObjectNode data = root.putObject("data");
    ArrayNode labelsNode = data.putArray("labels");
    for (String label : labels) {
      labelsNode.add(label);
    }

    ArrayNode datasets = data.putArray("datasets");
    int colorIdx = 0;
    for (GraphSet set : selectedSets) {
      ObjectNode ds = datasets.addObject();
      String color = GRAPH_COLORS.get(colorIdx % GRAPH_COLORS.size());
      colorIdx++;
      ds.put("label", set.label());
      ds.put("borderColor", color);
      ds.put("backgroundColor", color);
      ds.put("tension", 0.25);
      ds.put("pointRadius", 3);
      ds.put("pointHoverRadius", 5);
      ds.put("fill", false);

      ArrayNode dataNode = ds.putArray("data");
      ArrayNode absNode = ds.putArray("absCounts");
      Map<LocalDateTime, GraphPoint> values = valuesBySet.getOrDefault(set.id(), Map.of());
      for (LocalDateTime t : times) {
        GraphPoint gp = values.get(t);
        if (gp == null) {
          dataNode.addNull();
          absNode.addNull();
        } else {
          dataNode.add(gp.percent());
          absNode.add(gp.count());
        }
      }
    }

    ObjectNode options = root.putObject("options");
    options.put("responsive", true);
    options.put("maintainAspectRatio", false);

    ObjectNode plugins = options.putObject("plugins");
    ObjectNode legend = plugins.putObject("legend");
    legend.put("display", true);
    legend.put("position", "bottom");
    legend.putObject("labels").put("usePointStyle", true);

    ObjectNode title = plugins.putObject("title");
    title.put("display", true);
    title.put("text", getGraphTitle());
    title.putObject("font").put("family", "Heebo").put("size", 16).put("weight", "700");

    ObjectNode scales = options.putObject("scales");
    ObjectNode y = scales.putObject("y");
    y.put("min", 0);
    y.put("max", 100);
    y.putObject("ticks").put("stepSize", 10);
    ObjectNode x = scales.putObject("x");
    x.putObject("ticks").put("maxRotation", 55).put("minRotation", 45);

    try {
      return GRAPH_MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      return emptyLineChartJson(getGraphTitle());
    }
  }

  private String emptyLineChartJson(String title) {
    ObjectNode root = GRAPH_MAPPER.createObjectNode();
    root.put("type", "line");
    ObjectNode data = root.putObject("data");
    data.putArray("labels");
    data.putArray("datasets");
    ObjectNode options = root.putObject("options");
    options.put("responsive", true);
    options.put("maintainAspectRatio", false);
    ObjectNode plugins = options.putObject("plugins");
    ObjectNode t = plugins.putObject("title");
    t.put("display", true);
    t.put("text", title);
    try {
      return GRAPH_MAPPER.writeValueAsString(root);
    } catch (Exception e) {
      return "{}";
    }
  }

  private static LocalDateTime resolveRunStart(ExtentSummary summary, long fallbackMillis) {
    LocalDateTime start = null;
    if (summary != null && summary.run() != null) {
      start = parseExtentTime(summary.run().startTime());
    }
    return start != null ? start : toLocalDateTime(fallbackMillis);
  }

  private static GraphPoint toGraphPoint(ExtentSummary summary, Set<String> selectedStatuses) {
    if (summary == null || summary.totals() == null) return null;
    ExtentSummary.Totals t = summary.totals();
    int total = Math.max(0, t.total());
    if (total == 0) return null;
    int count = 0;
    if (selectedStatuses.contains("FAIL")) count += t.fail();
    if (selectedStatuses.contains("SKIP")) count += t.skip();
    if (selectedStatuses.contains("KNOWN_BUG")) count += t.knownBug();
    if (selectedStatuses.contains("PASS")) count += t.pass();
    double pct = (count * 100.0) / total;
    return new GraphPoint(round1(pct), count, total);
  }

  private static double round1(double value) {
    return Math.round(value * 10.0) / 10.0;
  }

  public List<String> getFilters() { return filters; }
  public String getActiveFilter() { return activeFilter; }
  public List<DashboardCard> getCards() { return cards; }
  public int getAutoRefreshSeconds() { return autoRefreshSeconds; }
  public boolean isAutoRefreshEnabled() { return autoRefreshSeconds > 0; }

  public record GraphGroup(String id, String name, List<GraphSet> items) implements Serializable {}

  public record GraphSet(
          String id,
          String label,
          String filterName,
          String baseDir,
          String dirNameRegex
  ) implements Serializable {}

  public record StatusOption(String value, String label) implements Serializable {}

  private record GraphPoint(double percent, int count, int total) {}
}
