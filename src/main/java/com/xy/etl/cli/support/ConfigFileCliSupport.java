package com.xy.etl.cli.support;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class ConfigFileCliSupport {

    private static final String CONFIG_PROPERTY = "config";

    private ConfigFileCliSupport() {
    }

    public static boolean isCliMode() {
        return !getConfigPaths().isEmpty();
    }

    public static List<String> getConfigPaths() {
        String configProp = System.getProperty(CONFIG_PROPERTY);
        if (configProp == null || configProp.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(configProp.split(","))
                .map(String::trim)
                .filter(part -> !part.isEmpty())
                .toList();
    }
}
