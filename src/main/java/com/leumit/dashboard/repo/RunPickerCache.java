package com.leumit.dashboard.repo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

@Slf4j
@Component
public class RunPickerCache {

  private static final String CREATE_TABLE_SQL = """
          CREATE TABLE IF NOT EXISTS run_pick_cache (
              base_dir TEXT NOT NULL,
              dir_pattern TEXT NOT NULL,
              dir_flags INTEGER NOT NULL,
              cached_at INTEGER NOT NULL,
              matched_dirs INTEGER NOT NULL,
              candidates_json TEXT NOT NULL,
              PRIMARY KEY (base_dir, dir_pattern, dir_flags)
          )
          """;

  private static final String SELECT_SQL = """
          SELECT cached_at, matched_dirs, candidates_json
          FROM run_pick_cache
          WHERE base_dir = ? AND dir_pattern = ? AND dir_flags = ?
          """;

  private static final String UPSERT_SQL = """
          INSERT INTO run_pick_cache (base_dir, dir_pattern, dir_flags, cached_at, matched_dirs, candidates_json)
          VALUES (?, ?, ?, ?, ?, ?)
          ON CONFLICT(base_dir, dir_pattern, dir_flags) DO UPDATE SET
            cached_at = excluded.cached_at,
            matched_dirs = excluded.matched_dirs,
            candidates_json = excluded.candidates_json
          """;

  private static final String DELETE_SQL = """
          DELETE FROM run_pick_cache
          WHERE base_dir = ? AND dir_pattern = ? AND dir_flags = ?
          """;

  private static final TypeReference<List<CandidateEntry>> CANDIDATE_LIST_TYPE =
          new TypeReference<>() {};

  private final ObjectMapper objectMapper;
  private final Path dbPath;
  private final long ttlMillis;
  private final ReentrantLock lock = new ReentrantLock();

  public RunPickerCache(
          ObjectMapper objectMapper,
          @Value("${dashboard.cache.dbPath:${user.home}/.extent-dashboard/report-cache.sqlite}") String dbPath,
          @Value("${dashboard.cache.runPickerTtlMillis:30000}") long ttlMillis
  ) {
    this.objectMapper = objectMapper;
    this.dbPath = Paths.get(dbPath).toAbsolutePath().normalize();
    this.ttlMillis = ttlMillis;
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

  public Optional<CachedCandidates> get(Path baseDir, Pattern dirNamePattern) {
    if (ttlMillis <= 0 || baseDir == null || dirNamePattern == null) {
      return Optional.empty();
    }

    String baseKey = normalizeBaseDir(baseDir);
    String pattern = dirNamePattern.pattern();
    int flags = dirNamePattern.flags();
    long now = System.currentTimeMillis();

    lock.lock();
    try (Connection conn = openConnection();
         PreparedStatement ps = conn.prepareStatement(SELECT_SQL)) {
      ps.setString(1, baseKey);
      ps.setString(2, pattern);
      ps.setInt(3, flags);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) return Optional.empty();

        long cachedAt = rs.getLong("cached_at");
        if (ttlMillis > 0 && now - cachedAt > ttlMillis) {
          deleteRow(conn, baseKey, pattern, flags);
          return Optional.empty();
        }

        int matchedDirs = rs.getInt("matched_dirs");
        String json = rs.getString("candidates_json");
        List<CandidateEntry> candidates = objectMapper.readValue(json, CANDIDATE_LIST_TYPE);
        if (candidates == null) {
          candidates = List.of();
        }
        return Optional.of(new CachedCandidates(cachedAt, matchedDirs, candidates));
      }
    } catch (Exception e) {
      log.warn("RunPickerCache read failed: baseDir={} err={}", baseDir, e.getMessage());
      return Optional.empty();
    } finally {
      lock.unlock();
    }
  }

  public void put(Path baseDir, Pattern dirNamePattern, CachedCandidates cached) {
    if (ttlMillis <= 0 || baseDir == null || dirNamePattern == null || cached == null) {
      return;
    }

    String baseKey = normalizeBaseDir(baseDir);
    String pattern = dirNamePattern.pattern();
    int flags = dirNamePattern.flags();

    lock.lock();
    try (Connection conn = openConnection();
         PreparedStatement ps = conn.prepareStatement(UPSERT_SQL)) {
      ps.setString(1, baseKey);
      ps.setString(2, pattern);
      ps.setInt(3, flags);
      ps.setLong(4, cached.cachedAt());
      ps.setInt(5, cached.matchedDirs());
      ps.setString(6, objectMapper.writeValueAsString(cached.candidates()));
      ps.executeUpdate();
    } catch (Exception e) {
      log.warn("RunPickerCache write failed: baseDir={} err={}", baseDir, e.getMessage());
    } finally {
      lock.unlock();
    }
  }

  private static String normalizeBaseDir(Path baseDir) {
    return baseDir.toAbsolutePath().normalize().toString();
  }

  private void deleteRow(Connection conn, String baseDir, String pattern, int flags) {
    try (PreparedStatement ps = conn.prepareStatement(DELETE_SQL)) {
      ps.setString(1, baseDir);
      ps.setString(2, pattern);
      ps.setInt(3, flags);
      ps.executeUpdate();
    } catch (SQLException ignored) {
      // best-effort cleanup
    }
  }

  private Connection openConnection() throws SQLException {
    String url = "jdbc:sqlite:" + dbPath;
    return DriverManager.getConnection(url);
  }

  public record CandidateEntry(String runDir, String reportPath, long lastModifiedMillis) {}

  public record CachedCandidates(long cachedAt, int matchedDirs, List<CandidateEntry> candidates) {}
}
