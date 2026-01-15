package com.leumit.dashboard.web;

import com.leumit.dashboard.config.DashboardFiltersProperties;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.nio.file.*;
import java.util.Objects;

@RestController
public class RunAssetController {

    private final DashboardFiltersProperties props;

    public RunAssetController(DashboardFiltersProperties props) {
        this.props = props;
    }

    @GetMapping("/run-asset")
    public ResponseEntity<Resource> asset(
            @RequestParam String filter,
            @RequestParam String item,
            @RequestParam String run,
            @RequestParam String path
    ) throws Exception {

        Path runDir = resolveRunDir(filter, item, run);

        // Only allow under runDir
        Path file = runDir.resolve(path).normalize().toAbsolutePath();
        if (!file.startsWith(runDir)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        // Optionally restrict to screenshots only
        // if (!file.toString().contains("screenshots")) return ResponseEntity.status(403).build();

        if (!Files.exists(file) || Files.isDirectory(file)) {
            return ResponseEntity.notFound().build();
        }

        Resource res = new UrlResource(file.toUri());
        String ct = Files.probeContentType(file);
        MediaType mt = (ct != null) ? MediaType.parseMediaType(ct) : MediaType.APPLICATION_OCTET_STREAM;

        return ResponseEntity.ok()
                .contentType(mt)
                .cacheControl(CacheControl.noCache())
                .body(res);
    }

    private Path resolveRunDir(String filterName, String itemTitle, String runFolder) {
        var f = props.getFilters().stream()
                .filter(x -> Objects.equals(x.getName(), filterName))
                .findFirst()
                .orElseThrow();

        var it = f.getItems().stream()
                .filter(x -> Objects.equals(x.getTitle(), itemTitle))
                .findFirst()
                .orElseThrow();

        Path base = Path.of(it.getBaseDir()).normalize().toAbsolutePath();
        Path dir = base.resolve(runFolder).normalize().toAbsolutePath();

        if (!dir.startsWith(base)) throw new IllegalArgumentException("Invalid run path");
        if (!Files.isDirectory(dir)) throw new IllegalArgumentException("Run dir not found");

        return dir;
    }
}
