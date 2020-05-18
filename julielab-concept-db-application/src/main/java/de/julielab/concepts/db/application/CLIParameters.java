package de.julielab.concepts.db.application;

import org.kohsuke.args4j.Option;

import java.io.File;
import java.util.List;

public class CLIParameters {

    @Option(name = "-v", forbids = "-nv", aliases = {"--versioning"}, usage = "Sets the database version according to the configuration file. Can only been done once. After setting the version, imports or operations on the database will be rejected.")
    public boolean doVersioning;

    @Option(name = "-nv", forbids = "-v", aliases = {"--no-versioning"}, usage = "Supresses the database versioning that happens by default after importing data and executing operators. Useful when applying multiple configurations onto the same database. Eventually, a database should almost always get a version for documentation purposes.")
    public boolean noVersioning;

    @Option(name = "-i", aliases = {"--import"}, handler = OptionalStringArrayOptionHandler.class, usage = "Creates and import concepts into the database. The names of concept creators can optionally be given as whitespace separated arguments. Only these creators will be called.", metaVar = "[concept creator names]")
    public List<String> doImport;

    @Option(name = "-o", aliases = {"--operation"}, handler = OptionalStringArrayOptionHandler.class, usage = "Performs operations on the database. The names of operators can optionally be given as whitespace separated arguments. Only these operators will be called.", metaVar = "[operator names]")
    public List<String> doOperation;

    @Option(name = "-e", aliases = {"--export"}, handler = OptionalStringArrayOptionHandler.class, usage = "Performs data export from the database. The names of exporters can optionally be given as whitespace separated arguments. Only these exporters will be called.", metaVar = "[exporter names]")
    public List<String> doExport;

    @Option(name = "-a", aliases = {"--all"}, handler = OptionalStringArrayOptionHandler.class, usage = "Imports concepts, performs the configured operations and the data exports and finally sets the given version to the database. The names of concept creators, operators and exportes can optionally be given as whitespace separated arguments. Only these steps will be called.", metaVar = "[step names]")
    public List<String> doAll;

    @Option(name = "-c", aliases = {"--configuration"}, usage = "The XML configuration file defining all settings and actions to perform on the database.", metaVar = "configuration file", required = true)
    public File configurationFile;
}
