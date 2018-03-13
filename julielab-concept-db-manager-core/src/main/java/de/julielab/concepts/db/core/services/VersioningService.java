package de.julielab.concepts.db.core.services;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.ServiceLoader;

import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.core.VersioningConstants;
import de.julielab.concepts.db.core.spi.Versioning;
import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.concepts.util.VersionRetrievalException;
import de.julielab.concepts.util.VersioningException;

public class VersioningService implements ParameterExposing {
	
	
	private static final Logger log = LoggerFactory.getLogger(VersioningService.class);
	
	private HierarchicalConfiguration<ImmutableNode> connectionConfiguration;
	private ServiceLoader<Versioning> loader;
	private static Map<HierarchicalConfiguration<ImmutableNode>, VersioningService> serviceMap;

	private VersioningService(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		this.connectionConfiguration = connectionConfiguration;
		loader = ServiceLoader.load(Versioning.class);
	}

	public static synchronized VersioningService getInstance(HierarchicalConfiguration<ImmutableNode> connectionConfiguration) {
		if (serviceMap == null)
			serviceMap = new HashMap<>();
		return serviceMap.computeIfAbsent(connectionConfiguration, VersioningService::new);
	}
	
	public void setVersion(HierarchicalConfiguration<ImmutableNode> versioningConfig) throws VersioningException {
		Iterator<Versioning> iterator = loader.iterator();
		while (iterator.hasNext()) {
			Versioning versioning = iterator.next();
			try {
				versioning.setConnection(connectionConfiguration);
				versioning.setVersion(versioningConfig);
			} catch (ConceptDatabaseConnectionException e) {
				log.debug("Versioning " + versioning.getClass().getCanonicalName() + " could not serve connection " + ConfigurationUtils.toString(connectionConfiguration) + ": {}", e.getMessage());
				log.trace("Full stack trace:", e);
			}
		}
	}

	public String getVersion() throws VersionRetrievalException {
		Iterator<Versioning> iterator = loader.iterator();
		while (iterator.hasNext()) {
			Versioning versioning = iterator.next();
			try {
				versioning.setConnection(connectionConfiguration);
				return versioning.getVersion();
			} catch (ConceptDatabaseConnectionException e) {
				log.debug("Versioning " + versioning.getClass().getCanonicalName() + " could not serve connection " + ConfigurationUtils.toString(connectionConfiguration) + ": {}", e.getMessage());
				log.trace("Full stack trace:", e);
			}
		}
		return null;
	}

	@Override
	public void exposeParameters(String basePath, HierarchicalConfiguration<ImmutableNode> template) {
		Iterator<Versioning> iterator = loader.iterator();
		while (iterator.hasNext()) {
			Versioning versioning = iterator.next();
			versioning.exposeParameters(basePath, template);
		}
	}
}
