package com.leumit.dashboard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ExtentSummary(Run run, Totals totals, List<FeatureSummary> features) {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Run(String startTime, String endTime) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Totals(int pass, int fail, int knownBug, int skip, int total, double passRate) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record FeatureSummary(String name, String displayName, List<String> path, String status, boolean knownBug) {}
}
