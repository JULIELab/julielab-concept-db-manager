package de.julielab.concepts.db.creators;

import de.julielab.concepts.db.core.DefaultFacetCreator;
import de.julielab.concepts.db.core.services.FacetCreationService;
import de.julielab.concepts.db.core.spi.ConceptCreator;
import de.julielab.concepts.util.ConceptCreationException;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.java.utilities.ConfigurationUtilities;
import de.julielab.java.utilities.FileUtilities;
import de.julielab.neo4j.plugins.datarepresentation.*;
import de.julielab.neo4j.plugins.datarepresentation.constants.FacetConstants;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class HgncGroupsConceptCreator implements ConceptCreator {
    public static final String FAMILYFILE = "familyfile";
    public static final String FAMILYALIASFILE = "familyaliasfile";
    public static final String HIERARCHYFILE = "hierarchyfile";
    public static final String GENETOGROUPMAP = "genetogroupmap";
    public static final String HGNC_SOURCE = "HGNCG";
    public static final String HGNC_GROUP_LABEL = "HGNC_GROUP";
    private final static Logger log = LoggerFactory.getLogger(HgncGroupsConceptCreator.class);

    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig) throws ConceptCreationException, FacetCreationException {
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        try {
            ConfigurationUtilities.checkParameters(importConfig, slash(confPath, FAMILYFILE), slash(confPath, FAMILYALIASFILE), slash(confPath, HIERARCHYFILE), slash(confPath, GENETOGROUPMAP));
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }
        File familyFile = new File(importConfig.getString(slash(confPath, FAMILYFILE)));
        File familyAliasFile = new File(importConfig.getString(slash(confPath, FAMILYALIASFILE)));
        File hierarchyFile = new File(importConfig.getString(slash(confPath, HIERARCHYFILE)));
        File geneToGroupMap = new File(importConfig.getString(slash(confPath, GENETOGROUPMAP)));
        // since the group IDs and the NCBI gene IDs are just numbers, we use two different maps to store them
        Map<String, ImportConcept> id2groupconcept = new HashMap<>();
        Map<String, ImportConcept> id2geneconcept = new HashMap<>();
        try {
            createGroupConcepts(familyFile, id2groupconcept);
            addGroupAliases(familyAliasFile, id2groupconcept);
            addGroupHierarchyRelations(hierarchyFile, id2groupconcept);
            addNCBIGeneLinks(geneToGroupMap, id2groupconcept, id2geneconcept);
        } catch (IOException e) {
            throw new ConceptCreationException(e);
        }

        ImportFacet facet = FacetCreationService.getInstance().createFacet(importConfig);
        ImportOptions options = new ImportOptions();
        options.createHollowAggregateElements = true;
        options.doNotCreateHollowParents = false;
        ImportConcepts importConcepts = new ImportConcepts(Stream.concat(id2groupconcept.values().stream(), id2geneconcept.values().stream()), facet);
        importConcepts.setNumConcepts(id2groupconcept.size() + id2geneconcept.size());
        log.info("Created a total of {} concepts where {} concepts are gene groups and {} are genes identified by NCBI Gene ID.", importConcepts.getNumConcepts(), id2groupconcept.size(), id2geneconcept.size());
        importConcepts.setImportOptions(options);
        return Stream.of(importConcepts);
    }

    /**
     * <pre>
     * HGNC ID	NCBI Gene ID(supplied by NCBI)	Gene group ID	NCBI Gene ID
     * HGNC:5297	3359	172	3359
     * HGNC:5298	9177	172	9177
     * HGNC:24003	170572	172	170572
     * </pre>
     *
     * @param geneToGroupMap
     * @param id2concept
     * @param id2geneconcept
     * @throws IOException
     */
    private void addNCBIGeneLinks(File geneToGroupMap, Map<String, ImportConcept> id2concept, Map<String, ImportConcept> id2geneconcept) throws IOException {
        try (final BufferedReader br = FileUtilities.getReaderFromFile(geneToGroupMap)) {
            final CSVParser parse = CSVFormat.Builder.create(CSVFormat.RFC4180).setHeader().setSkipHeaderRecord(true).setDelimiter('\t').build().parse(br);
            for (CSVRecord record : parse) {
                final String hgncId = record.get("HGNC ID");
                final String ncbiGeneIdFromNCBI = record.get("NCBI Gene ID(supplied by NCBI)");
                final String ncbiGeneId = record.get("NCBI Gene ID");
                final String geneGroupIds = record.get("Gene group ID");
                // sometimes one of the NCBI Gene fields is not given, this is why we use both
                final String finalNcbiGeneId = ncbiGeneId.isBlank() ? ncbiGeneIdFromNCBI : ncbiGeneId;
                // withdrawn HGNC symbols do not have a gene group mapping
                if (geneGroupIds.isBlank() || finalNcbiGeneId.isBlank())
                    continue;
                final ImportConcept concept = new ImportConcept(new ConceptCoordinates(finalNcbiGeneId, "NCBI Gene", finalNcbiGeneId, "NCBI Gene"));
                concept.additionalProperties = new HashMap<>();
                concept.additionalProperties.put("hgnc_id", hgncId);
                for (String geneGroupId : geneGroupIds.split("\\|")) {
                    final ImportConcept groupConcept = id2concept.get(geneGroupId);
                    if (groupConcept == null)
                        throw new IllegalStateException("Inconsistent input data: the gene to group map file defines the group ID " + geneGroupId + " but this ID is not contained in the family file.");

                    concept.addParent(groupConcept.coordinates);
                }

                id2geneconcept.put(finalNcbiGeneId, concept);
            }
        }

    }

    /**
     * <pre>
     * "parent_fam_id","child_fam_id"
     * "639","640"
     * "639","642"
     * "16","18"
     * "16","19"
     * "19","20"
     * "19","21"
     * "16","24"
     * </pre>
     *
     * @param hierarchyFile
     * @param id2concept
     * @throws IOException
     */
    private void addGroupHierarchyRelations(File hierarchyFile, Map<String, ImportConcept> id2concept) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(hierarchyFile)) {
            final CSVParser parse = getCsvParserWithHeader(br);
            for (CSVRecord record : parse) {
                final String parentFamId = record.get("parent_fam_id");
                final String childId = record.get("child_fam_id");
                final ImportConcept parentConcept = id2concept.get(parentFamId);
                if (parentConcept == null)
                    throw new IllegalStateException("Inconsistent input data: the hierarchy file defines the parent group ID " + parentFamId + " but this ID is not contained in the family file.");
                final ImportConcept childConcept = id2concept.get(childId);
                if (childConcept == null)
                    throw new IllegalStateException("Inconsistent input data: the hierarchy file defines the child group ID " + parentFamId + " but this ID is not contained in the family file.");
                if (childConcept.parentCoordinates.isEmpty())
                    childConcept.parentCoordinates = new ArrayList<>();
                childConcept.parentCoordinates.add(parentConcept.coordinates);
            }
        }
    }

    /**
     * <pre>
     * "id","family_id","alias"
     * "223","423","Galactosylgalactosylxylosylprotein 3-beta-glucuronosyltransferases"
     * "224","423","Glucuronosyltransferases, type I"
     * "225","423","Uridine diphosphate glucuronic acid:acceptor glucuronosyltransferases"
     * "1246","1205","DPP4 activity and/or structural homolog family"
     * "2094","139","G protein coupled receptors"
     * </pre>
     *
     * @param familyAliasFile
     * @param id2concept
     */
    private void addGroupAliases(File familyAliasFile, Map<String, ImportConcept> id2concept) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(familyAliasFile)) {
            CSVParser parse = getCsvParserWithHeader(br);
            for (CSVRecord record : parse) {
                String familyId = record.get("family_id");
                // trim the alias, there were a few cases with trailing white spaces
                String alias = record.get("alias").trim();
                ImportConcept concept = id2concept.get(familyId);
                if (concept == null)
                    throw new IllegalStateException("Inconsistent input data: The family alias file contains an alias for gene group " + familyId + " but this group was not contained in the family file.");
                if (concept.synonyms.isEmpty())
                    concept.synonyms = new ArrayList<>();
                concept.synonyms.add(alias);
            }
        }
    }

    /**
     * <pre>
     * "id","abbreviation","name","external_note","pubmed_ids","desc_comment","desc_label","desc_source","desc_go","typical_gene"
     * "871","HOMER","Homer scaffold proteins ","","23913637","NULL","NULL","NULL","NULL","HOMER1"
     * "1130","YPEL","Yippee like family","","15556292","NULL","NULL","NULL","NULL","YPEL1"
     * "887","IGK","Immunoglobulin kappa","","NULL","NULL","NULL","NULL","NULL","NULL"
     * "84","ZMAT","Zinc fingers matrin-type","","NULL","NULL","NULL","NULL","NULL","ZMAT5"
     * </pre>
     *
     * @param familyFile
     * @param id2concept
     * @throws IOException
     */
    private void createGroupConcepts(File familyFile, Map<String, ImportConcept> id2concept) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(familyFile)) {
            CSVParser parse = getCsvParserWithHeader(br);
            for (CSVRecord record : parse) {
                String groupId = record.get("id");
                final String abbreviation = record.get("abbreviation");
                String groupName = record.get("name");
                final String externalNote = record.get("external_note");
                final String descComment = record.get("desc_comment");
                ImportConcept concept = new ImportConcept(new ConceptCoordinates(groupId, HGNC_SOURCE, groupId, HGNC_SOURCE));
                concept.addGeneralLabel(HGNC_GROUP_LABEL);
                if (abbreviation.isBlank())
                    concept.prefName = groupName;
                else {
                    concept.prefName = abbreviation;
                    concept.synonyms = new ArrayList<>();
                    concept.synonyms.add(groupName);
                }
                concept.descriptions = new ArrayList<>();
                if (!externalNote.isBlank() && !"NULL".equalsIgnoreCase(externalNote))
                    concept.descriptions.add(externalNote);
                if (!descComment.isBlank() && !"NULL".equalsIgnoreCase(descComment))
                    concept.descriptions.add(descComment);
                id2concept.put(groupId, concept);
            }
        }
    }

    private CSVParser getCsvParserWithHeader(BufferedReader br) throws IOException {
        return CSVFormat.Builder.create(CSVFormat.RFC4180).setHeader().setSkipHeaderRecord(true).build().parse(br);
    }

    @Override
    public String getName() {
        return HgncGroupsConceptCreator.class.getSimpleName();
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        String base = slash(basePath, CONCEPTS, CREATOR, CONFIGURATION);
        template.addProperty(slash(basePath, CONCEPTS, CREATOR, NAME), getName());
        template.addProperty(slash(base, FAMILYFILE), "");
        template.addProperty(slash(base, HIERARCHYFILE), "");
        template.addProperty(slash(base, GENETOGROUPMAP), "");
        FacetCreationService.getInstance().exposeParameters(basePath, template);
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, FACET_GROUP, NAME), "Biology");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, NAME), "Protein Complexes");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, DefaultFacetCreator.SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);
    }
}
