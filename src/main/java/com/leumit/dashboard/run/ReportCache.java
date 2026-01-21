package com.leumit.dashboard.run;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.leumit.dashboard.model.ExtentSummary;
import com.leumit.dashboard.run.SparkHtmlReportParser.Feature;
import com.leumit.dashboard.run.SparkHtmlReportParser.Scenario;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.sql.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Component
public class ReportCache {

    private static final String CREATE_TABLE_SQL = """
            CREATE TABLE IF NOT EXISTS report_cache (
                report_path TEXT PRIMARY KEY,
                last_modified INTEGER NOT NULL,
                report_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """;

    private static final String SELECT_SQL =
            "SELECT last_modified, report_json FROM report_cache WHERE report_path = ?";
    private static final String UPSERT_SQL = """
            INSERT INTO report_cache (report_path, last_modified, report_json, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(report_path) DO UPDATE SET
              last_modified = excluded.last_modified,
              report_json = excluded.report_json,
              updated_at = excluded.updated_at
            """;

    private final ObjectMapper objectMapper;
    private final Path dbPath;
    private final ReentrantLock lock = new ReentrantLock();

    public ReportCache(
            ObjectMapper objectMapper,
            @Value("${dashboard.cache.dbPath:${user.home}/.extent-dashboard/report-cache.sqlite}") String dbPath
    ) {
        this.objectMapper = objectMapper;
        this.dbPath = Paths.get(dbPath).toAbsolutePath().normalize();
    }

    @PostConstruct
    public void init() {
        try {
            Path parent = dbPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (Connection conn = openConnection();
                 Statement st = conn.createStatement()) {
                st.executeUpdate(CREATE_TABLE_SQL);
            }
        } catch (Exception e) {
            log.warn("ReportCache init failed: {}", e.getMessage(), e);
        }
    }

    public CachedReport getOrLoad(Path reportHtml) throws IOException {
        if (reportHtml == null) {
            throw new IllegalArgumentException("reportHtml is null");
        }
        Path path = reportHtml.toAbsolutePath().normalize();
        FileTime lm = Files.getLastModifiedTime(path);
        long lastModified = lm.toMillis();

        CachedReport cached = read(path.toString(), lastModified);
        if (cached != null) {
            if (needsDescriptionRefresh(cached.features())) {
                List<Feature> features = SparkHtmlReportParser.parseFeaturesNoLogs(path);
                CachedReport refreshed = new CachedReport(cached.summary(), features);
                write(path.toString(), lastModified, refreshed);
                return refreshed;
            }
            return cached;
        }

        ExtentSummary summary = SparkHtmlReportParser.parseSummary(path);
        List<Feature> features = SparkHtmlReportParser.parseFeaturesNoLogs(path);
        CachedReport fresh = new CachedReport(summary, features);
        write(path.toString(), lastModified, fresh);
        return fresh;
    }

    public ExtentSummary getSummary(Path reportHtml) throws IOException {
        return getOrLoad(reportHtml).summary();
    }

    public List<Feature> getFeatures(Path reportHtml) throws IOException {
        return getOrLoad(reportHtml).features();
    }

    public Map<String, String> getScenarioStatuses(Path reportHtml) throws IOException {
        CachedReport report = getOrLoad(reportHtml);
        return buildScenarioStatuses(report.features());
    }

    private CachedReport read(String reportPath, long lastModified) {
        lock.lock();
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
            ps.setString(1, reportPath);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                long cachedLm = rs.getLong("last_modified");
                if (cachedLm != lastModified) return null;

                String json = rs.getString("report_json");
                return objectMapper.readValue(json, CachedReport.class);
            }
        } catch (Exception e) {
            log.warn("ReportCache read failed: path={} err={}", reportPath, e.getMessage());
            return null;
        } finally {
            lock.unlock();
        }
    }

    private void write(String reportPath, long lastModified, CachedReport report) {
        lock.lock();
        try (Connection conn = openConnection();
             PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
            ps.setString(1, reportPath);
            ps.setLong(2, lastModified);
            ps.setString(3, toJson(report));
            ps.setLong(4, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (Exception e) {
            log.warn("ReportCache write failed: path={} err={}", reportPath, e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private String toJson(CachedReport report) throws JsonProcessingException {
        return objectMapper.writeValueAsString(report);
    }

    private Connection openConnection() throws SQLException {
        String url = "jdbc:sqlite:" + dbPath;
        return DriverManager.getConnection(url);
    }

    private static Map<String, String> buildScenarioStatuses(List<Feature> features) {
        Map<String, String> out = new HashMap<>();
        if (features == null || features.isEmpty()) return out;

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
                nameCounts.merge(clean(s.name()), 1, Integer::sum);
            }

            Map<String, Integer> nameSeen = new HashMap<>();
            for (Scenario s : f.scenarios()) {
                String sName = clean(s.name());
                int ordinal = nameSeen.merge(sName, 1, Integer::sum);
                boolean duplicate = nameCounts.getOrDefault(sName, 0) > 1;
                String keyName = duplicate ? (sName + " #" + ordinal) : sName;
                String scenarioKey = RunHistoryAnalyzer.scenarioKey(normalizedPath, keyName);
                if (!scenarioKey.isBlank()) {
                    out.put(scenarioKey, s.status());
                }
            }
        }

        return out;
    }

    private static boolean needsDescriptionRefresh(List<Feature> features) {
        if (features == null || features.isEmpty()) return false;
        for (Feature f : features) {
            if (f.descriptionHtml() == null) return true;
        }
        return false;
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

    public record CachedReport(ExtentSummary summary, List<Feature> features) {}
}
