package de.julielab.concepts.db.creators.mesh;

import com.google.common.collect.LinkedHashMultimap;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.exchange.DataImporter;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.neo4j.plugins.datarepresentation.*;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

public class MeshConceptCreator implements ConceptCreator {

    public static final String INPUT = "input";
    public static final String XMLFILE = "xmlfile";
    public static final String FORMAT = "format";
    public static final String FACETGROUP = "facetgroup";
    public static final String ORG_ID_REGEX = "originalidregex";
    public static final String ORG_SOURCE = "originalsource";
    public static final String SOURCE_NAME = "sourcename";


    private final static Logger log = LoggerFactory.getLogger(MeshConceptCreator.class);
    public static Set<String> FORMATS = new HashSet<>(Arrays.asList("MESH_XML", "MESH_SUPPLEMENTARY_XML", "SIMPLE_XML"));

    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig) throws ConceptCreationException, FacetCreationException {
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        Tree conceptTree;
        String facetGroupName;
        try {
            facetGroupName = ConfigurationUtilities.requirePresent(slash(confPath, FACETGROUP), importConfig::getString);
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }
        // These matchers will get the concept's original ID and look for a match. The same is done for the original source.
        // Each matcher is queried in the given order. The first matcher to return a non-null value will also terminate the query process.
        // Each matcher can also have a "non-match" source, allowing for "either-or" decisions.
        // Lastly, a matcher may just be given the (original) source without a regular expression. In this case, the source will always be returned by the matcher.
        Map<String, ConceptSourceMatcher> conceptSourceMatchers = importConfig.configurationsAt(slash(confPath, INPUT))
                .stream()
                .map(ConceptSourceMatcher::new)
                .collect(Collectors.toMap(m -> m.getSourceFile(), m -> m));

        Map<Descriptor, String> desc2Source = new HashMap<>();
        try {
            conceptTree = new Tree("Concepts Tree");
            for (HierarchicalConfiguration<ImmutableNode> input : importConfig.configurationsAt(slash(confPath, INPUT))) {
                String file = ConfigurationUtilities.requirePresent(XMLFILE, input::getString);
                String format = ConfigurationUtilities.requirePresent(FORMAT, input::getString);
                if (!FORMATS.contains(format))
                    throw new ConceptCreationException("Unknown concept XML format: " + format + ". Supported formats: " + FORMATS);
                log.info("Adding data from {} to the internal concept hierarchy representation that will be inserted into the database.");
                switch (format) {
                    case "MESH_XML": {
                        List<Descriptor> descriptors = DataImporter.fromOriginalMeshXml(file, conceptTree, true);
                        descriptors.forEach(d -> desc2Source.put(d, file));
                        break;
                    }
                    case "MESH_SUPPLEMENTARY_XML": {
                        List<Descriptor> descriptors = DataImporter.fromSupplementaryConceptsXml(file, conceptTree);
                        descriptors.forEach(d -> desc2Source.put(d, file));
                        break;
                    }
                    case "SIMPLE_XML": {
                        List<Descriptor> descriptors = DataImporter.fromUserDefinedMeshXml(file, conceptTree);
                        descriptors.forEach(d -> desc2Source.put(d, file));
                        break;
                    }
                }
            }

            conceptTree.verifyIntegrity();
            log.info("Concept XML data reading finished.");
        } catch (Exception e) {
            throw new ConceptCreationException(e);
        }

        return createConceptsFromTree(conceptTree, facetGroupName, desc2Source, conceptSourceMatchers).stream();

    }

    private List<ImportConcepts> createConceptsFromTree(Tree tree, String facetGroupName, Map<Descriptor, String> desc2Source, Map<String, ConceptSourceMatcher> conceptSourceMatchers) throws ConceptCreationException, FacetCreationException {
        // Sanity check.
        List<Descriptor> rootChildren2 = tree.childDescriptorsOf(tree.getRootDesc());
        for (Descriptor facet : rootChildren2) {
            if (facet.getName().startsWith("Facet"))
                ;// System.out.println(facet.getUI() + " is facet");
            else
                throw new ConceptCreationException(
                        "There is at least one top-level node - i.e. a node directly under the technical root node - that is no facet: "
                                + facet);
        }

        FacetsProvider facetsProvider = new FacetsProvider(tree);

        // get all descriptors sorted by it's heights
        List<Descriptor> allDesc = tree.getAllDescriptorsByHeight();

        // root node should be the first one, so just delete it
        if (allDesc.get(0).equals(tree.getRootDesc())) {
            allDesc.remove(0);
        } else {
            log.error(
                    "ERROR: implementation of getAllDescriptorsByHeight has changed. Cannot remove root descriptor anymore.");
        }

        log.info("Organizing descriptors by facet, i.e. each descriptor's MeSH root term...");
        ProgressCounter counter = new ProgressCounter(allDesc.size(), 10000, "descriptor");
        counter.startMsg();
        // Use a LINKEDHashMultimap to keep the import order constant over
        // multiple imports
        LinkedHashMultimap<String, Descriptor> facet2Desc = LinkedHashMultimap.create();
        List<Descriptor> facetDescriptors = tree.childDescriptorsOf(tree.getRootDesc());
        for (Descriptor desc : allDesc) {
            Set<String> facets = facetsProvider.getFacets(desc);

            // Is this perhaps a facet itself?
            if (facetDescriptors.contains(desc))
                continue;

            for (String facet : facets) {
                facet2Desc.put(facet, desc);
            }
            counter.inc();
        }
        counter.finishMsg();

        // only using linked hash maps does not seem to keep import order
        // constant. So just sort everything before
        // importing to somehow reach a constant import order
        List<String> facetNames = new ArrayList<>(facet2Desc.keySet());
        Collections.sort(facetNames);

        List<ImportConcepts> importConcepts = new ArrayList<>(facet2Desc.size());

        for (String facetName : facetNames) {
            Collection<Descriptor> descriptorsInFacet = facet2Desc.get(facetName);
            List<Descriptor> sortedDescriptorsInFacet = new ArrayList<>(descriptorsInFacet);
            Collections.sort(sortedDescriptorsInFacet);
            counter = new ProgressCounter(sortedDescriptorsInFacet.size(), 1000, "Semedico term");
            counter.startMsg();
            log.info("Converting descriptors for facet {} to Semedico terms...", facetName);
            ImportFacet importFacet;
            importFacet = FacetsProvider.createMeshImportFacet(facetName, facetGroupName, 0);

            List<ImportConcept> concepts = new ArrayList<>();
            ImportConcepts conceptsWithFacet = new ImportConcepts(concepts, importFacet);
            // Allow hollow parents because some parents are distributed into
            // other facets.
            conceptsWithFacet.setImportOptions(new ImportOptions(true));
            importConcepts.add(conceptsWithFacet);
            for (Descriptor desc : sortedDescriptorsInFacet) {
                String termId = desc.getUI();
                String preferredName = desc.getPrefConcept().getPrefTerm().getName();
                List<String> synonyms = desc.getSynonymNames();
                String description = desc.getScopeNote();
                List<ConceptCoordinates> parents = new ArrayList<>();

                // Determine parents.
                List<Descriptor> parentDescriptors = tree.parentDescriptorsOf(desc);
                for (Descriptor parentDescriptor : parentDescriptors) {
                    // Exclude the facet nodes, they are no concepts.
                    if (!facetDescriptors.contains(parentDescriptor)) {
                        parents.add(getConceptCoordinate(parentDescriptor, conceptSourceMatchers, desc2Source));
                    }
                }
                ImportConcept term = new ImportConcept(preferredName, synonyms, Arrays.asList(description), getConceptCoordinate(desc, conceptSourceMatchers, desc2Source), parents);
                concepts.add(term);

                counter.inc();
            }
            counter.finishMsg();
        }
        return importConcepts;
    }


    /**
     * <p>
     * Creates the concept coordinates for the given descriptor.
     * </p>
     * <p>To determine the original source - if any - the concept source matcher for the descriptor's source file will
     * be fetched via <tt>desc2Source</tt> and <tt>conceptSourceMatchers</tt>. The matcher will then match the
     * descriptor's UID. If this is successful (either because there was a regular expression match oder no expression
     * was given at all but the original source was given anyway), the original source is set to the output of the
     * matcher. The current source is always set to the one that was configured and that is also stored in the matcher.
     * </p>
     *
     * @param desc
     * @param conceptSourceMatchers
     * @param desc2Source
     * @return
     */
    private ConceptCoordinates getConceptCoordinate(Descriptor desc, Map<String, ConceptSourceMatcher> conceptSourceMatchers, Map<Descriptor, String> desc2Source) {
        String sourceFile = desc2Source.get(desc);
        ConceptSourceMatcher conceptSourceMatcher = conceptSourceMatchers.get(sourceFile);
        String source = conceptSourceMatcher.getSource();
        String originalSource = conceptSourceMatcher.matchOriginalId(desc.getUI());
        return new ConceptCoordinates(desc.getUI(), source, originalSource != null ? desc.getUI() : null, originalSource, true);
    }

    @Override
    public String getName() {
        return getClass().getSimpleName();
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        String confPath = slash(basePath, CONCEPTS, CREATOR, CONFIGURATION);
        template.addProperty(slash(basePath, CONCEPTS, CREATOR, NAME), getName());
        template.addProperty(slash(confPath, FACETGROUP), "");
        template.addProperty(slash(confPath, INPUT, XMLFILE), "");
        template.addProperty(slash(confPath, INPUT, FORMAT), "");
        template.addProperty(slash(confPath, INPUT, SOURCE_NAME), "");
        template.addProperty(slash(confPath, INPUT, ORG_ID_REGEX), "");
        template.addProperty(slash(confPath, INPUT, ORG_SOURCE), "");
    }
}
