package com.leumit.dashboard.run;

import java.io.Serializable;
import java.util.*;

public final class RunModel implements Serializable {

    private final RunNode root;                 // ROOT -> GROUP* -> FEATURE -> SCENARIO -> STEP
    private final Map<String, RunNode> byId;    // fast lookup for selection
    private final int featureCount;

    public RunModel(RunNode root, Map<String, RunNode> byId, int featureCount) {
        this.root = Objects.requireNonNull(root, "root");
        this.byId = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(byId, "byId")));
        this.featureCount = featureCount;
    }

    public RunNode getRoot() { return root; }
    public Map<String, RunNode> getById() { return byId; }
    public int getFeatureCount() { return featureCount; }

    public enum Kind {
        ROOT, GROUP, FEATURE, SCENARIO, STEP, HOOK, NODE
    }

    public static final class RunNode implements Serializable {
        private final String id;
        private final Kind kind;
        private final String name;          // UI display (leaf name for feature/group)
        private final String fullName;      // original name (breadcrumb string etc.)
        private final String status;        // PASS/FAIL/WARNING/SKIP/...
        private final String bddType;       // extentreports bddType
        private final List<String> path;    // breadcrumb segments, if present/derived
        private final List<LogEntry> logs;  // logs for STEP/HOOK nodes
        private final String screenshotPath; // first media path found in logs (optional)
        private final List<RunNode> children;

        public RunNode(
                String id,
                Kind kind,
                String name,
                String fullName,
                String status,
                String bddType,
                List<String> path,
                List<LogEntry> logs,
                String screenshotPath,
                List<RunNode> children
        ) {
            this.id = id;
            this.kind = kind;
            this.name = name;
            this.fullName = fullName;
            this.status = status;
            this.bddType = bddType;
            this.path = path == null ? List.of() : List.copyOf(path);
            this.logs = logs == null ? List.of() : List.copyOf(logs);
            this.screenshotPath = screenshotPath;
            this.children = children == null ? List.of() : List.copyOf(children);
        }

        public String getId() { return id; }
        public Kind getKind() { return kind; }
        public String getName() { return name; }
        public String getFullName() { return fullName; }
        public String getStatus() { return status; }
        public String getBddType() { return bddType; }
        public List<String> getPath() { return path; }
        public List<LogEntry> getLogs() { return logs; }
        public String getScreenshotPath() { return screenshotPath; }
        public List<RunNode> getChildren() { return children; }
        public boolean isLeaf() { return children == null || children.isEmpty(); }
    }

    public record LogEntry(
            String timestamp,
            String status,
            String detailsHtml, // store as-is; render with escape="false"
            String mediaPath
    ) implements Serializable {}
}
