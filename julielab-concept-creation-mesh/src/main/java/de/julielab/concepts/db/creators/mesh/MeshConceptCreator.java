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
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class MeshConceptCreator implements ConceptCreator {

    public static final String MESHFILE = "meshfile";
    public static final String SUPPFILE = "suppfile";
    public static final String FACETGROUP = "facetgroup";

    public static final String SOURCE_MESH = "Medical Subject Headings";
    public static final String SOURCE_SUPP = "Medical Subject Headings, Supplementary Concepts";
    private final static Logger log = LoggerFactory.getLogger(MeshConceptCreator.class);

    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig) throws ConceptCreationException, FacetCreationException {
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        Tree mesh;
        String facetGroupName;
        try {
            facetGroupName = ConfigurationUtilities.requirePresent(slash(confPath, FACETGROUP), importConfig::getString);
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }
        try {
            String meshFile = ConfigurationUtilities.requirePresent(slash(confPath, MESHFILE), importConfig::getString);
            String suppFile = importConfig.getString(slash(confPath, SUPPFILE));
            mesh = new Tree("MeSH Descriptors and Supplementaries");
            log.info("Reading the MeSH XML descriptors from {}", meshFile);
            DataImporter.fromOriginalMeshXml(meshFile, mesh, true);
            if (!StringUtils.isBlank(suppFile)) {
                log.info("Reading the MeSH Supplementary concepts from {}", suppFile);
                DataImporter.fromSupplementaryConceptsXml(suppFile, mesh);
            }
            mesh.verifyIntegrity();
            log.info("MeSH file reading finished.");
        } catch (Exception e) {
            throw new ConceptCreationException(e);
        }


        return createConceptsFromTree(mesh, facetGroupName).stream();

    }

    private List<ImportConcepts> createConceptsFromTree(Tree tree, String facetGroupName) throws ConceptCreationException, FacetCreationException {
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
            // TODO repair
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
                        String parentId = parentDescriptor.getUI();
                        parents.add(getMeshConceptCoordinate(parentId));
                    }
                }
                ImportConcept term = new ImportConcept(preferredName, synonyms, Arrays.asList(description), getMeshConceptCoordinate(termId), parents);
                concepts.add(term);

                counter.inc();
            }
            counter.finishMsg();
        }
        return importConcepts;
    }

    /**
     * Returns an instance of {@link ConceptCoordinates} where source ID and original ID are set to the given
     * MeSH UID and <tt>uniqueSource</tt> is set to <tt>true</tt>. The source and original source is set to
     * {@link #SOURCE_MESH} for UIDs beginning with a <tt>D</tt> followed by a number and {@link #SOURCE_SUPP} otherwise.
     *
     * @param uid
     * @return
     */
    private ConceptCoordinates getMeshConceptCoordinate(String uid) {
        String source = uid.matches("D[0-9]+") ? source = SOURCE_MESH : SOURCE_SUPP;
        return new ConceptCoordinates(uid, source, uid, source, true);
    }

    @Override
    public String getName() {
      return  getClass().getSimpleName();
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        String confPath = slash(basePath, CONCEPTS, CREATOR, CONFIGURATION);
        template.addProperty(slash(basePath, CONCEPTS, CREATOR, NAME), getName());
        template.addProperty(slash(confPath, MESHFILE), "");
        template.addProperty(slash(confPath, SUPPFILE), "");
        template.addProperty(slash(confPath, FACETGROUP), "");
    }
}
