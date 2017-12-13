package de.julielab.concepts.db.core;

import java.util.List;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import de.julielab.concepts.db.core.spi.FacetCreator;
import de.julielab.concepts.util.FacetCreationException;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacet;
import de.julielab.neo4j.plugins.datarepresentation.ImportFacetGroup;

public class DefaultFacetCreator implements FacetCreator {

	public static final String PROVIDER_NAME = "default";

	public static final String FACET_GROUP_NAME = "facetgroup.name";
	public static final String NAME = "name";
	public static final String SHORT_NAME = "shortname";
	public static final String CUSTOM_ID = "customid";
	public static final String SOURCE_TYPE = "sourcetype";
	public static final String LABELS = "labels";
	public static final String NO_FACET = "nofacet";

	@Override
	public ImportFacet createFacet(HierarchicalConfiguration<ImmutableNode> facetConfiguration, Object facetData)
			throws FacetCreationException {
		String facetGroupName = facetConfiguration.getString(FACET_GROUP_NAME);
		String name = facetConfiguration.getString(NAME);
		String shortName = facetConfiguration.getString(SHORT_NAME);
		String customId = facetConfiguration.getString(CUSTOM_ID);
		String sourceType = facetConfiguration.getString(SOURCE_TYPE);
		List<String> labels = facetConfiguration.getList(String.class, LABELS);
		boolean noFacet = facetConfiguration.getBoolean(NO_FACET);

		ImportFacetGroup fg = new ImportFacetGroup(facetGroupName);
		return new ImportFacet(fg, customId, name, shortName, sourceType, labels, noFacet);
	}

	@Override
	public boolean hasName(String providername) {
		return providername.equals(getClass().getCanonicalName()) || providername.equals(PROVIDER_NAME);
	}

}