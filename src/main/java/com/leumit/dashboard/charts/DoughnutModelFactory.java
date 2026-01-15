package com.leumit.dashboard.charts;

import com.leumit.dashboard.model.ExtentSummary;
import software.xdev.chartjs.model.charts.DoughnutChart;
import software.xdev.chartjs.model.color.RGBAColor;
import software.xdev.chartjs.model.data.DoughnutData;
import software.xdev.chartjs.model.dataset.DoughnutDataset;
import software.xdev.chartjs.model.options.DoughnutOptions;
import software.xdev.chartjs.model.options.LegendOptions;
import software.xdev.chartjs.model.options.Plugins;

import java.math.BigDecimal;

public final class DoughnutModelFactory {

    private DoughnutModelFactory() {}

    public static String fromTotals(ExtentSummary.Totals t) {
        // Labels in Hebrew (as you requested)
        String pass = "עבר בהצלחה";
        String fail = "נכשל";
        String kb   = "באג ידוע";
        String skip = "דולג";

        DoughnutDataset ds = new DoughnutDataset()
                .setData(
                        BigDecimal.valueOf(t.pass()),
                        BigDecimal.valueOf(t.fail()),
                        BigDecimal.valueOf(t.knownBug()),
                        BigDecimal.valueOf(t.skip())
                )
                // sensible conventional colors; tweak anytime
                .addBackgroundColors(
                        new RGBAColor(34, 197, 94),    // pass
                        new RGBAColor(239, 68, 68),    // fail
                        new RGBAColor(245, 158, 11),   // known bug
                        new RGBAColor(148, 163, 184)   // skip
                );

        DoughnutOptions opts = new DoughnutOptions()
                .setResponsive(true)
                .setMaintainAspectRatio(false)
                .setPlugins(new Plugins()
                        // 3) remove chart legend
                        .setLegend(new LegendOptions().setDisplay(false))
                );

        return new DoughnutChart()
                .setData(new DoughnutData()
                        .addDataset(ds)
                        .setLabels(pass, fail, kb, skip)
                )
                .setOptions(opts)
                .toJson(); // PrimeFaces <p:chart> consumes this JSON string :contentReference[oaicite:1]{index=1}
    }
}
