package de.julielab.concepts.db.creators.mesh;

import com.google.common.collect.LinkedHashMultimap;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.tools.ProgressCounter;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcept;
import de.julielab.neo4j.plugins.datarepresentation.ImportConcepts;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Stream;

public class MeshConceptCreator implements ConceptCreator {

    private final static Logger logger = LoggerFactory.getLogger(MeshConceptCreator.class);

    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig) throws ConceptCreationException, FacetCreationException {
        // TODO implement
return null;
    }

    private List<ImportConcept> createConceptsFromTree(Tree tree) throws ConceptCreationException {
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
            logger.error(
                    "ERROR: implementation of getAllDescriptorsByHeight has changed. Cannot remove root descriptor anymore.");
        }

        logger.info("Organizing descriptors by facet, i.e. each descriptor's MeSH root term...");
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

        for (String facetName : facetNames) {
            Collection<Descriptor> descriptorsInFacet = facet2Desc.get(facetName);
            List<Descriptor> sortedDescriptorsInFacet = new ArrayList<>(descriptorsInFacet);
            Collections.sort(sortedDescriptorsInFacet);
            counter = new ProgressCounter(sortedDescriptorsInFacet.size(), 1000, "Semedico term");
            counter.startMsg();
            logger.info("Converting descriptors for facet {} to Semedico terms...", facetName);
            ImportFacet importFacet;
            // TODO repair
//            switch (importSource) {
//                case MESH_XML:
//                    importFacet = FacetsProvider.createMeshImportFacet(facetName);
//                    break;
//                case SEMEDICO_IMMUNOLY_DIR:
//                case SEMEDICO_IMMUNOLY_FILE:
//                case SEMEDICO_XML_DIR:
//                case SEMEDICO_XML_FILE:
//                    importFacet = FacetsProvider.createSemedicoImportFacet(facetName);
//                    break;
//                default:
//                    throw new IllegalArgumentException(
//                            "This term import algorithm is not applicable to import source " + importSource);
//            }
//            List<ImportTerm> importTerms = new ArrayList<>();
//            ImportTermAndFacet importTermAndFacet = new ImportTermAndFacet(importTerms, importFacet);
//            // Allow hollow parents because some parents are distributed into
//            // other facets.
//            importTermAndFacet.importOptions = new ImportOptions(true);
//            for (Descriptor desc : sortedDescriptorsInFacet) {
//                String termId = desc.getUI();
//                String preferredName = desc.getPrefConcept().getPrefTerm().getName();
//                List<String> synonyms = desc.getSynonymNames();
//                String description = desc.getScopeNote();
//                List<String> parents = new ArrayList<>();
//
//                // Determine parents.
//                List<Descriptor> parentDescriptors = tree.parentDescriptorsOf(desc);
//                for (Descriptor parentDescriptor : parentDescriptors) {
//                    // Exclude the facet nodes, they are no terms.
//                    if (!facetDescriptors.contains(parentDescriptor))
//                        parents.add(parentDescriptor.getUI());
//                }
//                ImportTerm term = new ImportTerm(preferredName, termId, description, synonyms, parents);
//                if (term.sourceId.matches("(D|C)[0-9]+")) {
//                    term.addGeneralLabel(ResourceTermLabels.IdMapping.ID_MAP_MESH.toString());
//                    // TODO This should be some kind of constant or identifier
//                    // to a "Source Node" in the database having
//                    // all
//                    // information about the actual source so it could be
//                    // displayed in Semedico.
//                    // Also, this value has to match the originalSource given to
//                    // MESH terms from BioPortal in
//                    // BioPortalTermImport for proper merging of the terms in
//                    // the Neo4j plugin.
//                    term.originalSource = "MESH";
//                    if (importSource == ImportSource.MESH_XML)
//                        term.source = "MeSH XML";
//                    else
//                        term.source = "Semedico Default Terms";
//                } else if (FacetsProvider.isImmunologyFacet(facetName)) {
//                    term.addGeneralLabel(ResourceTermLabels.IdMapping.ID_MAP_IMMUNOLOGY.toString());
//                    // TODO This should be some kind of constant or identifier
//                    // to a "Source Node" in the database having
//                    // all
//                    // information about the actual source so it could be
//                    // displayed in Semedico.
//                    term.source = "Semedico Immunology Terms";
//                    if (!term.sourceId.matches("(D|C)[0-9]+"))
//                        term.originalSource = "Semedico Immunology Terms";
//                } else {
//                    logger.warn("No ID map for term {} of source {}", termId, importSource);
//                    // the warning will apply to:
////					11:19:21 [main] WARN  d.j.s.mesh.exchange.DataExporter - No ID map for term dog of source SEMEDICO_XML_DIR
////					11:19:21 [main] WARN  d.j.s.mesh.exchange.DataExporter - No ID map for term human of source SEMEDICO_XML_DIR
////					11:19:21 [main] WARN  d.j.s.mesh.exchange.DataExporter - No ID map for term mouse of source SEMEDICO_XML_DIR
////					11:19:21 [main] WARN  d.j.s.mesh.exchange.DataExporter - No ID map for term rat of source SEMEDICO_XML_DIR
//                    // which should be okay since we use the NCBI Taxonomy for organisms
//                    // TODO This should be some kind of constant or identifier
//                    // to a "Source Node" in the database having
//                    // all
//                    // information about the actual source so it could be
//                    // displayed in Semedico.
//                    term.source = "Semedico Default Terms";
//                    if (!term.sourceId.matches("(D|C)[0-9]+"))
//                        term.originalSource = "Semedico Default Terms";
//                }
//                // All Semedico terms use original IDs in their source files
//                term.originalId = termId;
//
//                importTerms.add(term);
//
//                counter.inc();
//            }
//            counter.finishMsg();
//
//            logger.info("Importing facet {} and its {} terms into Neo4j on host {}.",
//                    new Object[] { facetName, importTerms.size(), termImportService.getDBHost() });
//            // HttpEntity response =
//            // neo4jAdapter.sendPostRequest(termImportService + "/" +
//            // Neo4jService.TERM_MANAGER_ENDPOINT
//            // + TermManager.INSERT_TERMS,
//            // importTermAndFacet.toNeo4jRestRequest());
//            String response = termImportService.importTerms(importTermAndFacet);
//            logger.info("Server responded: {}", response);
        }
        // TODO remove
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {

    }
}
