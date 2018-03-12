package de.julielab.concepts.db.core.spi;

import de.julielab.concepts.util.DatabaseOperationException;
import de.julielab.jssf.commons.spi.ExtensionPoint;
import de.julielab.jssf.commons.spi.ParameterExposing;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

public interface DatabaseOperator extends ExtensionPoint, DatabaseConnected, ParameterExposing {
    void operate(HierarchicalConfiguration<ImmutableNode> operationConfigration) throws DatabaseOperationException;

}
