package com.leumit.dashboard.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

@ConfigurationProperties(prefix = "dashboard")
public class DashboardFiltersProperties {

  private List<Filter> filters = new ArrayList<>();

  public List<Filter> getFilters() { return filters; }
  public void setFilters(List<Filter> filters) { this.filters = filters; }

  public static class Filter {
    private String name;
    private List<Item> items = new ArrayList<>();

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public List<Item> getItems() { return items; }
    public void setItems(List<Item> items) { this.items = items; }
  }

  public static class Item {
    private String title;
    private String baseDir;
    private String dirNameRegex;

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getBaseDir() { return baseDir; }
    public void setBaseDir(String baseDir) { this.baseDir = baseDir; }

    public String getDirNameRegex() { return dirNameRegex; }
    public void setDirNameRegex(String dirNameRegex) { this.dirNameRegex = dirNameRegex; }
  }
}
