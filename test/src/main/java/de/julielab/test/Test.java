package de.julielab.test;

import de.julielab.concepts.db.core.ConceptDBConfigurationTemplateGenerator;

import javax.naming.ConfigurationException;
import java.io.File;

public class Test {
    public static void main(String args[]) throws ConfigurationException, de.julielab.jssf.commons.util.ConfigurationException {
        ConceptDBConfigurationTemplateGenerator r = ConceptDBConfigurationTemplateGenerator.getInstance();
        r.writeConfigurationTemplate(new File("test.xml"));
    }
}
