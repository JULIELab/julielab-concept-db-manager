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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.julielab.concepts.db.core.ConfigurationConstants.*;
import static de.julielab.java.utilities.ConfigurationUtilities.slash;

public class FamPlexConceptCreator implements ConceptCreator {
    public static final String RELATIONSFILE = "relationsfile";
    public static final String GROUNDINGMAP = "groundingmap";
    public static final String NAMEEXTENSIONRECORDS = "nameextensionrecords";
    public static final String LABEL_FAMPLEX = "FPLX";
    private final static Logger log = LoggerFactory.getLogger(FamPlexConceptCreator.class);

    @Override
    public Stream<ImportConcepts> createConcepts(HierarchicalConfiguration<ImmutableNode> importConfig) throws ConceptCreationException, FacetCreationException {
        String confPath = slash(CONCEPTS, CREATOR, CONFIGURATION);
        try {
            ConfigurationUtilities.checkParameters(importConfig, slash(confPath, RELATIONSFILE), slash(confPath, GROUNDINGMAP), slash(confPath, NAMEEXTENSIONRECORDS));
        } catch (ConfigurationException e) {
            throw new ConceptCreationException(e);
        }
        File relationsFile = new File(importConfig.getString(slash(confPath, RELATIONSFILE)));
        File groundingMap = new File(importConfig.getString(slash(confPath, GROUNDINGMAP)));
        File nameextensionRecords = new File(importConfig.getString(slash(confPath, NAMEEXTENSIONRECORDS)));
        Map<String, ImportConcept> conceptsById = new HashMap<>();
        try {
            addConceptsFromNameExtensionRecords(conceptsById, nameextensionRecords);
            addConceptsFromFamplexRelationsFile(conceptsById, relationsFile);
            addConceptsFromFamplexGroundingMap(conceptsById, groundingMap);
        } catch (IOException e) {
            throw new ConceptCreationException(e);
        }

        ImportFacet facet = FacetCreationService.getInstance().createFacet(importConfig);
        ImportOptions options = new ImportOptions();
        options.createHollowAggregateElements = true;
        options.doNotCreateHollowParents = false;
        ImportConcepts importConcepts = new ImportConcepts(conceptsById.values().stream(), facet);
        importConcepts.setNumConcepts(conceptsById.size());
        log.info("Created a total of {} concepts.", importConcepts.getNumConcepts());
        importConcepts.setImportOptions(options);
        return Stream.of(importConcepts);
    }

    /**
     * <p>Adds FamPlex entity synonyms or writing variants that have been added using the SpecialistLexiconNameExpansion class
     * of the jcore-gene-mapper-ae/gene-mapper-resources project.</p>
     *
     * <p>The records look like this:
     * <pre>
     * GENO:6
     * bases:	H2B
     * inflections:	H2B, histone 2B
     * spellings:	histone 2B
     * acronyms:
     * abbreviations:	histone 2B
     * externalids:	FPLX:Histone_H2B
     * inputnames:	H2B, histone H2B
     * lexiconEuis:	E0732594, E0732595
     * &lt;newline&gt;
     * </pre>
     * </p>
     *
     * @param conceptsById
     * @param nameExtensionFile
     * @throws IOException
     */
    private void addConceptsFromNameExtensionRecords(Map<String, ImportConcept> conceptsById, File nameExtensionFile) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(nameExtensionFile)) {
            String line;
            Map<String, String> currentRecord = new HashMap<>();
            while ((line = br.readLine()) != null) {
                if (line.isBlank()) {
                    if (!currentRecord.isEmpty()) {
                        // newline; the old record is completely read, add enriched names to the ImportConcepts
                        addExtendedNamesToConcept(conceptsById, currentRecord);
                    }
                    currentRecord.clear();
                }
                String[] split = line.split("\t");
                if (split.length > 1)
                    currentRecord.put(split[0], split[1]);
            }
            // In case there was no newline after the last record
            if (!currentRecord.isEmpty())
                addExtendedNamesToConcept(conceptsById, currentRecord);
        }
    }

    private void addExtendedNamesToConcept(Map<String, ImportConcept> conceptsById, Map<String, String> currentRecord) {
        List<String> bases = null;
        List<String> inflections = Collections.emptyList();
        List<String> acronyms = Collections.emptyList();
        List<String> abbreviations = Collections.emptyList();
        List<String> spellings = Collections.emptyList();
        Optional<String> famplexId = Optional.empty();
        for (String key : currentRecord.keySet()) {
            if (key.startsWith("bases"))
                bases = Arrays.stream(currentRecord.get(key).split(", ")).collect(Collectors.toList());
            else if (key.startsWith("inflections"))
                inflections = Arrays.stream(currentRecord.get(key).split(", ")).collect(Collectors.toList());
            else if (key.startsWith("spellings"))
                spellings = Arrays.stream(currentRecord.get(key).split(", ")).collect(Collectors.toList());
            else if (key.startsWith("acronyms"))
                acronyms = Arrays.stream(currentRecord.get(key).split(", ")).collect(Collectors.toList());
            else if (key.startsWith("abbreviations"))
                abbreviations = Arrays.stream(currentRecord.get(key).split(", ")).collect(Collectors.toList());
            else if (key.startsWith("externalids"))
                famplexId = Arrays.stream(currentRecord.get(key).split(", ")).filter(id -> id.startsWith("FPLX:")).findAny();
        }
        if (famplexId.isPresent()) {
            String id = famplexId.get().split(":")[1];
            ImportConcept famplexConcept = conceptsById.compute(id, (k, v) -> v != null ? v : new ImportConcept(new ConceptCoordinates(id, "FPLX", id, "FPLX")));
            addNodeLabel(famplexConcept, LABEL_FAMPLEX);
            if (famplexConcept.prefName == null) {
                famplexConcept.prefName = bases.get(0);
                bases.remove(0);
            }
            if (famplexConcept.synonyms.isEmpty())
                famplexConcept.synonyms = new ArrayList<>();
            famplexConcept.synonyms.addAll(bases);
            famplexConcept.synonyms.addAll(acronyms);
            famplexConcept.synonyms.addAll(abbreviations);
            // there could be well some duplicates, make the entries distinct
            famplexConcept.synonyms = famplexConcept.synonyms.stream().distinct().collect(Collectors.toList());
            if (famplexConcept.writingVariants.isEmpty())
                famplexConcept.writingVariants = new ArrayList<>();
            famplexConcept.writingVariants.addAll(inflections);
            famplexConcept.writingVariants.addAll(spellings);
            // Remove duplicates in writingVariants
            famplexConcept.writingVariants = famplexConcept.writingVariants.stream().distinct().collect(Collectors.toList());
        }
    }

    /**
     * <p>Reads the FamPlex grounding map in TSV format and adds ImportConcepts for it.</p>
     * <p>We read only those entries that are associated with a FPLX ID.</p>
     * <p>
     * The file contents look like this:
     * <pre>
     * Na(+),K(+)-ATPase	FPLX	Na_K_ATPase
     * Na,K-ATPase beta subunit	FPLX	ATP1B
     * Na,K-ATPase β subunit	FPLX	ATP1B
     * Na,K-ATPase	FPLX	Na_K_ATPase
     * (9-1-1) complex	FPLX	9_1_1
     * 14-3-3 proteins	FPLX	p14_3_3
     * 14-3-3	FPLX	p14_3_3
     * 14-3-3zeta	UP	P63104
     * 4E-BP1	UP	Q13541
     * 4EBP	FPLX	EIF4EBP
     * 4EBP1	UP	Q13541
     * 53BP1	UP	Q12888
     * 9-1-1 complex	FPLX	9_1_1
     * 9-1-1	FPLX	9_1_1
     * a-adrenoreceptor	FPLX	ADRA
     *     </pre>
     * </p>
     *
     * @param conceptsById
     * @param groundingMap
     */
    private void addConceptsFromFamplexGroundingMap(Map<String, ImportConcept> conceptsById, File groundingMap) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(groundingMap)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\t");
                String fplxSource = split[1];
                if (!fplxSource.equals("FPLX"))
                    continue;
                String synonym = split[0];
                String fplxId = split[2];
                // Just in case this method is called after conceptsById has already been populated from other files, check if there is already an ImportConcept for this ID
                ImportConcept famplexConcept = conceptsById.compute(fplxId, (k, v) -> v != null ? v : new ImportConcept(new ConceptCoordinates(fplxId, fplxSource, fplxId, fplxSource)));
                addNodeLabel(famplexConcept, LABEL_FAMPLEX);
                if (famplexConcept.synonyms.isEmpty())
                    famplexConcept.synonyms = new ArrayList<>();
                famplexConcept.synonyms.add(synonym);
                if (famplexConcept.prefName == null)
                    famplexConcept.prefName = fplxId;
            }
        }
    }

    /**
     * <p>Reads lines from the EG-converted FamPlex relations file in TSV format into ImportConcept instances.</p>
     * <p>The EG-conversion happens in the resource creation of the jcore-gene-mapper-ae/gene-mapper-resources project.
     * The HGNC IDs are mapped to NCBI Gene/Entrez Gene IDs there.</p>
     * <p>The file contents look like this:
     * <pre>
     * EG	1978	isa	FPLX	EIF4EBP
     * EG	1979	isa	FPLX	EIF4EBP
     * EG	8637	isa	FPLX	EIF4EBP
     * EG	623	isa	FPLX	BDKR
     * EG	624	isa	FPLX	BDKR
     * EG	58	partof	FPLX	F_actin
     * EG	59	partof	FPLX	F_actin
     * EG	60	partof	FPLX	F_actin
     * EG	70	partof	FPLX	F_actin
     * EG	71	partof	FPLX	F_actin
     * </pre>
     * </p>
     *
     * @param relationsFile
     * @throws IOException
     */
    private void addConceptsFromFamplexRelationsFile(Map<String, ImportConcept> conceptsById, File relationsFile) throws IOException {
        try (BufferedReader br = FileUtilities.getReaderFromFile(relationsFile)) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\t");
                String sourceIdSource = split[0].equals("EG") ? "NCBI Gene" : split[0];
                String sourceId = split[1];
                String relation = split[2];
                String fplxSource = split[3];
                String fplxId = split[4];
                if (!fplxSource.equals("FPLX")) {
                    log.warn("Omitting non-FamPlex ID mapping target line {}", line);
                    continue;
                }

                // the "source concept" - meant is the left side of the relation - could either be from NCBI Gene (originally
                // HGNC, but we expect the converted file version here) of from FamPlex itself.
                ImportConcept sourceConcept = conceptsById.compute(sourceId, (k, v) -> v != null ? v : new ImportConcept(new ConceptCoordinates(sourceId, sourceIdSource, sourceId, sourceIdSource)));
                if (sourceIdSource.equals("FPLX"))
                    addNodeLabel(sourceConcept, LABEL_FAMPLEX);

                // Just in case this method is called after conceptsById has already been populated from other files, check if there is already an ImportConcept for this ID
                ImportConcept famplexConcept = conceptsById.compute(fplxId, (k, v) -> v != null ? v : new ImportConcept(new ConceptCoordinates(fplxId, fplxSource, fplxId, fplxSource)));
                if (famplexConcept.prefName == null)
                    famplexConcept.prefName = fplxId;
                // Add the FAMPLEX label to all the FamPlex entities so we can easily find them later in the DB.
                addNodeLabel(famplexConcept, LABEL_FAMPLEX);

                // The relationship is either "isa" or "partof". In the ConceptDB the parent-child relationship is
                // "is broader than" to roughly denote a semantically "larger" and a "smaller" unit. This should hold
                // here, so add the right relationship side as a parent.
                sourceConcept.addParent(famplexConcept.coordinates);
                // Also, add the exact relationship.
                sourceConcept.addRelationship(new ImportConceptRelationship(famplexConcept.coordinates, relation));
            }
        }
    }

    private void addNodeLabel(ImportConcept famplexConcept, String label) {
        if (famplexConcept.generalLabels == null || !famplexConcept.generalLabels.contains(LABEL_FAMPLEX))
            famplexConcept.addGeneralLabel(LABEL_FAMPLEX);
    }

    @Override
    public String getName() {
        return FamPlexConceptCreator.class.getSimpleName();
    }

    @Override
    public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
        String base = slash(basePath, CONCEPTS, CREATOR, CONFIGURATION);
        template.addProperty(slash(basePath, CONCEPTS, CREATOR, NAME), getName());
        template.addProperty(slash(base, RELATIONSFILE), "");
        template.addProperty(slash(base, GROUNDINGMAP), "");
        template.addProperty(slash(base, NAMEEXTENSIONRECORDS), "");
        FacetCreationService.getInstance().exposeParameters(basePath, template);
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, FACET_GROUP, NAME), "Biology");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, NAME), "Protein Complexes");
        template.setProperty(slash(basePath, FACET, CREATOR, CONFIGURATION, DefaultFacetCreator.SOURCE_TYPE), FacetConstants.SRC_TYPE_HIERARCHICAL);
    }
}
