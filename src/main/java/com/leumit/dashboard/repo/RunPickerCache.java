package com.leumit.dashboard.repo;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

@Slf4j
@Component
public class RunPickerCache {

  private static final String CREATE_TABLE_SQL = """
          CREATE TABLE IF NOT EXISTS run_pick_entry (
              base_dir TEXT NOT NULL,
              dir_pattern TEXT NOT NULL,
              dir_flags INTEGER NOT NULL,
              run_dir TEXT NOT NULL,
              report_path TEXT NOT NULL,
              last_modified INTEGER NOT NULL,
              PRIMARY KEY (base_dir, dir_pattern, dir_flags, run_dir)
          )
          """;

  private static final String SELECT_SQL = """
          SELECT run_dir, report_path, last_modified
          FROM run_pick_entry
          WHERE base_dir = ? AND dir_pattern = ? AND dir_flags = ?
          """;

  private static final String UPSERT_SQL = """
          INSERT INTO run_pick_entry (base_dir, dir_pattern, dir_flags, run_dir, report_path, last_modified)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(base_dir, dir_pattern, dir_flags, run_dir) DO UPDATE SET
            report_path = excluded.report_path,
            last_modified = excluded.last_modified
          """;

  private final Path dbPath;
  private final ReentrantLock lock = new ReentrantLock();

  public RunPickerCache(
          @Value("${dashboard.cache.dbPath:${user.home}/.extent-dashboard/report-cache.sqlite}") String dbPath
  ) {
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
      log.warn("RunPickerCache init failed: {}", e.getMessage(), e);
    }
  }

  public Map<String, CandidateEntry> loadEntries(Path baseDir, Pattern dirNamePattern) {
    if (baseDir == null || dirNamePattern == null) {
      return Map.of();
    }

    String baseKey = normalizeBaseDir(baseDir);
    String pattern = dirNamePattern.pattern();
    int flags = dirNamePattern.flags();
    Map<String, CandidateEntry> out = new HashMap<>();

    lock.lock();
    try (Connection conn = openConnection();
         PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
      ps.setString(1, baseKey);
      ps.setString(2, pattern);
      ps.setInt(3, flags);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String runDir = rs.getString("run_dir");
          String reportPath = rs.getString("report_path");
          long lastModified = rs.getLong("last_modified");
          if (runDir == null || reportPath == null) continue;
          out.put(runDir, new CandidateEntry(runDir, reportPath, lastModified));
        }
      }
    } catch (Exception e) {
      log.warn("RunPickerCache read failed: baseDir={} err={}", baseDir, e.getMessage());
    } finally {
      lock.unlock();
    }

    return out;
  }

  public void putAll(Path baseDir, Pattern dirNamePattern, List<CandidateEntry> entries) {
    if (baseDir == null || dirNamePattern == null || entries == null || entries.isEmpty()) {
      return;
    }

    String baseKey = normalizeBaseDir(baseDir);
    String pattern = dirNamePattern.pattern();
    int flags = dirNamePattern.flags();

    lock.lock();
    try (Connection conn = openConnection();
         PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
      conn.setAutoCommit(false);
      for (CandidateEntry entry : entries) {
        ps.setString(1, baseKey);
        ps.setString(2, pattern);
        ps.setInt(3, flags);
        ps.setString(4, entry.runDir());
        ps.setString(5, entry.reportPath());
        ps.setLong(6, entry.lastModifiedMillis());
        ps.addBatch();
      }
      ps.executeBatch();
      conn.commit();
    } catch (Exception e) {
      log.warn("RunPickerCache write failed: baseDir={} err={}", baseDir, e.getMessage());
    } finally {
      lock.unlock();
    }
  }

  static String normalizeBaseDir(Path baseDir) {
    return baseDir.toAbsolutePath().normalize().toString();
  }

  static String normalizeRunDir(Path runDir) {
    return runDir.toAbsolutePath().normalize().toString();
  }

  private Connection openConnection() throws SQLException {
    String url = "jdbc:sqlite:" + dbPath;
    return DriverManager.getConnection(url);
  }

  public record CandidateEntry(String runDir, String reportPath, long lastModifiedMillis) {}
}
