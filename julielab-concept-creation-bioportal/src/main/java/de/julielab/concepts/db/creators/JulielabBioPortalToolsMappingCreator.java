package de.julielab.concepts.db.creators;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import de.julielab.bioportal.ontologies.BioPortalToolConstants;
import de.julielab.bioportal.ontologies.data.OntologyClassMapping;
import de.julielab.bioportal.util.BioPortalToolUtils;
import de.julielab.concepts.db.core.spi.MappingCreator;
import de.julielab.concepts.util.ConceptDBManagerRuntimeException;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.MappingCreationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.datarepresentation.ImportMapping;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.dot;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class
JulielabBioPortalToolsMappingCreator implements MappingCreator {

    public static final String ALLOWED_ACRONYMS = "allowedacronyms";
    private final static Logger log = LoggerFactory.getLogger(JulielabBioPortalToolsMappingCreator.class);

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        template.addProperty(slash(basePath, MAPPINGS, CREATOR, NAME), getName());
        template.addProperty(slash(basePath, MAPPINGS, CREATOR, CONFIGURATION, PATH), "");
        template.addProperty(slash(basePath, MAPPINGS, CREATOR, CONFIGURATION, ALLOWED_ACRONYMS), "");
    }

    @Override
    public Stream<ImportMapping> createMappings(HierarchicalConfiguration<ImmutableNode> importConfig) throws MappingCreationException {

        try {
            ConfigurationUtilities.checkParameters(importConfig, dot(MAPPINGS, CREATOR, CONFIGURATION, PATH), dot(MAPPINGS, CREATOR, CONFIGURATION, ALLOWED_ACRONYMS));
            ConfigurationUtilities.checkFilesExist(importConfig, dot(MAPPINGS, CREATOR, CONFIGURATION, PATH));
            String pathToMappings = importConfig.getString(dot(MAPPINGS, CREATOR, CONFIGURATION, PATH));
            final Set<Object> allowedAcronyms = new HashSet<>(importConfig.getList(dot(MAPPINGS, CREATOR, CONFIGURATION, ALLOWED_ACRONYMS)));
            log.info("Importing mappings from {}{}", pathToMappings, allowedAcronyms != null && !allowedAcronyms.isEmpty() ? " for acronyms " + allowedAcronyms : "");
            File file = new File(pathToMappings);
            Stream<ImportMapping> mappings = Stream.empty();
            if (file.isFile()) {
                log.info("Found mapping file \"{}\", processing...", file.getAbsolutePath());
                return readMappingFile(file).stream();
            } else {
                File[] files = file.listFiles(new FilenameFilter() {

                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(BioPortalToolConstants.MAPPING_EXT) || name
                                .endsWith(BioPortalToolConstants.MAPPING_EXT + ".gz");
                    }
                });
                log.info("Got directory with {} mapping files.", files != null ? files.length : 0);
                Stream.of(files).parallel().filter(mappingFile -> {
                    String acronym = BioPortalToolUtils.getAcronymFromFileName(mappingFile);
                    if (null != allowedAcronyms && !allowedAcronyms.isEmpty() && !allowedAcronyms.contains(acronym)) {
                        log.debug(
                                "Skipping mappings for ontology with acronym {} because it is not contained in the set of allowed acronyms.",
                                acronym);
                        return false;
                    }
                    return true;
                }).flatMap(mappingFile -> {
                    log.info("Processing mapping file {}", mappingFile.getAbsolutePath());
                    try {
                        return readMappingFile(mappingFile).stream();
                    } catch (IOException e) {
                        throw new ConceptDBManagerRuntimeException(e);
                    }
                });

                return mappings;
            }
        } catch (ConfigurationException e) {
            throw new MappingCreationException(e);
        } catch (IOException e) {
            throw new MappingCreationException(e);
        }
    }

    /**
     * @param file A file containing a JSON list of mapping objects according to the model given by the
     *             {@link OntologyClassMapping} class.
     * @throws IOException
     */
    private List<ImportMapping> readMappingFile(File file) throws IOException {
        Gson gson = new Gson();
        Reader reader = FileUtilities.getReaderFromFile(file);
        Type mappingListType = new TypeToken<List<OntologyClassMapping>>() {//
        }.getType();
        List<OntologyClassMapping> mappings = gson.fromJson(reader, mappingListType);

        List<ImportMapping> importMappings = new ArrayList<>(mappings.size());
        for (OntologyClassMapping mapping : mappings) {
            String type = mapping.source;
            // TODO this means ignoring user-delivered mappings! We just do this for the moment to lower data load since
            // we don't know how to deal with those currently
            if (mapping.process != null)
                continue;
            if (null == type)
                throw new IllegalArgumentException("Could not find the mapping type for mapping " + mapping);

            if (type.equals("SAME_URI"))
                // This kind of "mapping" is handled automatically by the ConceptManager plugin.
                continue;

            if (mapping.classes.size() != 2)
                throw new IllegalArgumentException("Mapping occurred that does not map exactly two classes " + mapping);

            String id1 = mapping.classes.get(0).id;
            String id2 = mapping.classes.get(1).id;

            ImportMapping importMapping = new ImportMapping(id1, id2, type);
            importMappings.add(importMapping);
        }

        log.info(
                "Importing {} mappings after filtering unwanted mapping types like SAME_URI and user-defined mappings (for the time being).",
                importMappings.size());
        return importMappings;
    }

    @Override
    public void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) throws ConceptDatabaseConnectionException {
        // Ignored; this creator does not require a database connection but just reads precomputed mappings from file.
    }

    @Override
    public String getName() {
        return "JulielabBioPortalToolsMappingCreator";
    }
}
