package de.julielab.concepts.db.creators.mesh.exchange;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import org.slf4j.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.julielab.concepts.db.creators.mesh.Tree;
import de.julielab.concepts.db.creators.mesh.modifications.DescAdditions;

/**
 * This class deals with importing data for into a <code>Tree</code> object from various sources:
 * <ul>
 * <li>MeSH XML</li>
 * <li>UD-XML (data generated by previously used algorithm to create semedico-MeSH</li>
 * <li>Own XML (adaption of MeSH-XML)</li>
 * <li>more to come ...</li>
 * </ul>
 *
 * @author Philipp Lucas
 */
public class DataImporter {

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(DataImporter.class);

    /**
     * Imports descriptors following the MeSH XML syntax from a file. Previous content of data will not be overwritten.
     *
     * @param xmlFilepath Path to XML file.
     * @param data        Tree instance to import data to.
     * @throws Exception
     */
    public static void fromOriginalMeshXml(String xmlFilepath, Tree data) throws Exception {
        fromOriginalMeshXml(xmlFilepath, data, false);
    }

    public static List<Descriptor> fromOriginalMeshXml(String xmlFilepath, Tree data, boolean createMeshFacets) throws Exception {
        logger.info("# Importing descriptor records from MeSH XML file '" + xmlFilepath + "' ... ");

        Parser4Mesh saxHandler;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(true);

            SAXParser parser = factory.newSAXParser();
            XMLReader xmlReader = parser.getXMLReader();

            // BufferedReader reader = new BufferedReader(new FileReader(xmlFilepath), 8192*8);
            // InputSource inputSource = new InputSource(reader);
            InputStream is = new FileInputStream(new File(xmlFilepath));
            if (xmlFilepath.endsWith("gz") || xmlFilepath.endsWith("gzip"))
                is = new GZIPInputStream(is);
            InputSource inputSource = new InputSource(is);
            inputSource.setSystemId(new File(xmlFilepath).getCanonicalPath()); // to resolve relative dtd file in xml
            // file

            saxHandler = new Parser4Mesh(data, createMeshFacets);
            xmlReader.setContentHandler(saxHandler);

            xmlReader.parse(inputSource);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            logger.error(e.getMessage());
            throw e;
        }
        logger.info("# ... done.");
        return saxHandler.getCreatedDescriptors();
    }

    /**
     * Imports MeSH Supplementary Concept Records following the appropriate XML syntax from a file. Previous content of
     * data will not be overwritten.
     *
     * @param xmlFilepath Path to XML file.
     * @param data        Tree instance to import data to.
     * @throws Exception
     */
    public static List<Descriptor> fromSupplementaryConceptsXml(String xmlFilepath, Tree data) throws Exception {
        logger.info("# Importing descriptor records from MeSH XML file '" + xmlFilepath + "' ... ");

        Parser4SupplementaryConcepts saxHandler;
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setValidating(false);
            factory.setNamespaceAware(true);

            SAXParser parser = factory.newSAXParser();
            XMLReader xmlReader = parser.getXMLReader();

            // BufferedReader reader = new BufferedReader(new FileReader(xmlFilepath), 8192*8);
            // InputSource inputSource = new InputSource(reader);
            InputStream is = new FileInputStream(new File(xmlFilepath));
            if (xmlFilepath.endsWith("gz") || xmlFilepath.endsWith("gzip"))
                is = new GZIPInputStream(is);
            InputSource inputSource = new InputSource(is);
            inputSource.setSystemId(new File(xmlFilepath).getCanonicalPath()); // to resolve relative dtd file in xml
            // file

            saxHandler = new Parser4SupplementaryConcepts(data);
            xmlReader.setContentHandler(saxHandler);

            xmlReader.parse(inputSource);

        } catch (ParserConfigurationException | SAXException | IOException e) {
            logger.error(e.getMessage());
            throw e;
        }
        logger.info("# ... done.");
        return saxHandler.getCreatedDescriptors();
    }

    /**
     * <p>
     * Imports descriptors following the user defined MeSH XML syntax from a file. Previous content of data will not be
     * overwritten. 'user defined' in this context refers to the XML format that was previously used as an intermediate
     * format for importing MeSH data into the semedico DBMS.
     * </p>
     *
     * @param xmlDirPath Path to directory with XML files. Each file refers to a named 'facet' (see XML), and each such facet
     *                   is represented by a descriptor with one tree vertex. Hence, all tree vertices of that file are (not
     *                   necessarily direct) children of this 'facet tree vertex'.
     * @param newData    Tree instance to import data to.
     * @throws IOException
     * @throws SAXException
     */
    public static List<Descriptor> fromUserDefinedMeshXml(String xmlDirPath, Tree newData) throws IOException, SAXException {

        logger.info("# Importing 'user defined' descriptor records from directory '" + xmlDirPath + "' ... ");

        File dir = new File(xmlDirPath);

        // read in data from all XML files in the directory xmlFilepath
        //			if (!xmlFile.getName().contains("symptoms")) {
        //				System.out.println("Excluding file " + xmlFile + " in DAtaImporter");
        //				continue;
        //			}
                Parser4UserDefMesh saxHandler = null;
        File[] files;
        if (dir.isDirectory())
                files = dir.listFiles();
        else files = new File[] {dir};
        for (File xmlFile : files) {
            try {
                if (".".equals(xmlFile.getName()) || "..".equals(xmlFile.getName())) {
                    continue; // Ignore the self and parent aliases.
                }

                logger.info("# Importing 'user defined' descriptor records from XML file '" + xmlFile.getPath()
                        + "' ... ");

                XMLReader xmlReader = XMLReaderFactory.createXMLReader();
                // FileReader reader = new FileReader(xmlFile);
                // InputSource inputSource = new InputSource(reader);

                InputStream is = new FileInputStream(xmlFile);
                if (xmlFile.getName().endsWith("gz") || xmlFile.getName().endsWith("gzip"))
                    is = new GZIPInputStream(is);
                InputSource inputSource = new InputSource(is);

                saxHandler = new Parser4UserDefMesh(newData, xmlFile.getName());
                xmlReader.setContentHandler(saxHandler);

                xmlReader.parse(inputSource);

                is.close();
                // reader.close();

            } catch (FileNotFoundException e) {
                logger.error(e.getMessage());
                throw e;
            } catch (IOException e) {
                logger.error(e.getMessage());
                throw e;
            } catch (SAXException e) {
                logger.error(e.getMessage());
                throw e;
            }
        }
        logger.info("# ... done with directory '" + xmlDirPath + "'.");
        return saxHandler.getCreatedDescriptors();
    }

    /**
     * Imports descriptors following a file with the the "OwnXML" format.
     *
     * @param xmlFilepath Path to XML file.
     * @return Returns the imported descriptors.
     */
    public static DescAdditions fromOwnXML(String xmlFilepath) {
        logger.info("# Importing descriptor records from MeSH XML file '" + xmlFilepath + "' ... ");
        DescAdditions newDescs = null;
        try {

            XMLReader xmlReader = XMLReaderFactory.createXMLReader();
            FileReader reader = new FileReader(xmlFilepath);
            InputSource inputSource = new InputSource(reader);

            Parser4OwnMesh saxHandler = new Parser4OwnMesh();
            xmlReader.setContentHandler(saxHandler);

            xmlReader.parse(inputSource);

            newDescs = saxHandler.getNewDescriptors();

        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        } catch (SAXException e) {
            logger.error(e.getMessage());
        }
        logger.info("# ... done.");
        return newDescs;
    }

}
