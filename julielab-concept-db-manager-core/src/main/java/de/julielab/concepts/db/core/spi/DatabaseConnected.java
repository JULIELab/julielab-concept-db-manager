package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.ConceptDatabaseConnectionException;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

/**
 * An interface to indicate server provider interfaces that interact with the database and, thus, require a
 * database connection.
 */
public interface DatabaseConnected {
    /**
     * <p>Requires the implementing service provider to check if the given connection is supported and, if so, to use
     * it for later method calls. Please not that this kind of state requires caution in case of multithreading.</p>
     * <p>If the provider cannot serve to given connection configuration, it is required to throw a
     * {@link ConceptDatabaseConnectionException}.
     * This serves as the indication that the provider is not applicable for the required connection type.
     * Typically, it will then just be omitted by its service.</p>
     *
     * @param connectionConfiguration The connection details, e.g. HTTP, BOLT or file-based connection.
     * @throws ConceptDatabaseConnectionException If the provider does not support the passed connection configuration.
     */
    void setConnection(HierarchicalConfiguration<ImmutableNode> connectionConfiguration)
            throws ConceptDatabaseConnectionException;
}
