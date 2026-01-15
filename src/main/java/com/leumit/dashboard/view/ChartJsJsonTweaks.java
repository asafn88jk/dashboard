package com.leumit.dashboard.view;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public final class ChartJsJsonTweaks {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ChartJsJsonTweaks() {}

    public static String applyDashboardTweaks(String chartJson) {
        try {
            JsonNode node = MAPPER.readTree(chartJson);
            if (!(node instanceof ObjectNode root)) {
                return chartJson;
            }

            ObjectNode options = root.with("options");
            ObjectNode plugins = options.with("plugins");

            // 3) remove legend
            plugins.with("legend").put("display", false);

            // 5) tooltip bigger + Heebo
            ObjectNode tooltip = plugins.with("tooltip");
            tooltip.put("enabled", true);

            tooltip.with("titleFont")
                    .put("size", 15)
                    .put("weight", "600")
                    .put("family", "Heebo");

            tooltip.with("bodyFont")
                    .put("size", 15)
                    .put("weight", "600")
                    .put("family", "Heebo");

            // a bit more comfortable tooltip box
            tooltip.put("padding", 10);

            // optional: reduce animation jitter (nicer UX)
            options.put("animation", false);

            return MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            return chartJson; // fail safe: chart still renders
        }
    }
}
