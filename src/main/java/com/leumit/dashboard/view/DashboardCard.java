package com.leumit.dashboard.view;

import java.time.LocalDateTime;

public record DashboardCard(
        String title,
        int passPct,
        String duration,
        LocalDateTime startedAt,
        String doughnutJson,
        String filterName,
        String itemTitle,
        String runFolder
) {}
