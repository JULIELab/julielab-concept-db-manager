package de.julielab.concepts.db.core;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Atomic configuration key elements used to comprise the hierarchical keys used for the configuration.
 */
public class ConfigurationConstants {
    public static final String CONNECTION = "connection";
    public static final String URI = "uri";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String IMPORTS = "imports";
    public static final String IMPORT = "import";
    public final static String EXPORTS = "exports";
    public final static String EXPORT = "export";
    public static final String EXPORTER = "exporter";
    public static final String OPERATIONS = "operations";
    public static final String OPERATION = "operation";
    public static final String OPERATOR = "operator";
    public static final String CONFIGURATION = "configuration";
    public static final String PARAMETERS = "parameters";
    public static final String PLUGIN_NAME = "pluginname";
    public static final String PLUGIN_ENDPOINT = "pluginendpoint";
    public static final String PLUGIN = "plugin";
    public static final String CLASS = "class";
    public static final String METHOD = "method";
    public static final String VERSIONING = "versioning";

    /**
     * Convenience method for quick concatenation of hierarchical configuration keys.
     *
     * @param keys Configuration keys to concatenate into a single hierarchical key.
     * @return The input keys joined with dots.
     */
    public static String dot(String... keys) {
        return Stream.of(keys).collect(Collectors.joining("."));
    }

    /**
     * Convenience method for quick concatenation of hierarchical configuration keys into an XPath expression.
     *
     * @param keys Configuration keys to concatenate into a single hierarchical key.
     * @return The input keys joined with slashes for xpath expressions.
     */
    public static String slash(String... keys) {
        return Stream.of(keys).collect(Collectors.joining("/"));
    }

    private ConfigurationConstants() {
    }
}
