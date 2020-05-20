package de.julielab.concepts.db.core;

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
    public static final String CONCEPTS = "concepts";
    public static final String FACET = "facet";
    public static final String MAPPINGS = "mappings";
    public static final String FACET_GROUP = "facetgroup";
    public static final String CREATOR = "creator";
    public static final String NAME = "name";
    public static final String CONFIGURATION = "configuration";
    public static final String PARAMETERS = "parameters";
    public static final String PLUGIN_NAME = "pluginname";
    public static final String PLUGIN_ENDPOINT = "pluginendpoint";
    public static final String HTTP_METHOD = "httpmethod";
    public static final String SERVER_PLUGIN_INSERTER = "serverplugininserter";
    public static final String CLASS = "class";
    public static final String METHOD = "method";
    public static final String PATH = "path";
    public static final String DECODING = "decoding";
    public static final String OUTPUT_FILE = "outputfile";
    public static final String JSON2BYTEARRAY = "json2bytearray";
    public static final String BASE64 = "base64";
    public static final String GZIP = "gzip";
    public static final String CYPHER_QUERY = "cypherquery";
    public static final String VERSIONING = "versioning";
    public static final String VERSION = "version";

    private ConfigurationConstants() {
    }
}
