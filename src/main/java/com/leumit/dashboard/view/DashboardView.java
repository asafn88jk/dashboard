package com.leumit.dashboard.view;

import com.leumit.dashboard.config.DashboardFiltersProperties;
import com.leumit.dashboard.repo.RunPicker;
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
import java.time.LocalDateTime;
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

  public List<String> getFilters() { return filters; }
  public String getActiveFilter() { return activeFilter; }
  public List<DashboardCard> getCards() { return cards; }
  public int getAutoRefreshSeconds() { return autoRefreshSeconds; }
  public boolean isAutoRefreshEnabled() { return autoRefreshSeconds > 0; }
}
