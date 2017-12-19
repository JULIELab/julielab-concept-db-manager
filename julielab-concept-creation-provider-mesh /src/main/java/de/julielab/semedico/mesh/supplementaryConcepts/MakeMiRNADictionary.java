package de.julielab.semedico.mesh.supplementaryConcepts;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import com.google.common.collect.ListMultimap;

/**
 * creates a dictionary for microRNAs for the JenAge Project and
 * Cellerino/Baumgart. It will be used as supplement to the regex search
 * strategy, the aim is to identify miRNA - coding Genes relations.
 * 
 * @author modersohn
 * 
 */
public class MakeMiRNADictionary {

	private String input;
	private String output;

	public MakeMiRNADictionary(String input, String output) {
		super();
		this.input = input;
		this.output = output;
	}

	/**
	 * creates the XML dictionary file from the token map
	 * iterates through the map with the ids and synonymes
	 * an entry can look like that: 
	 * 	C568221; [MIRN590 microRNA, human; miR-390, human]
	 * 		C568221 is the unique id
	 * 		MIRN590 is one name of the microRNA
	 * 		human is the species
	 * the entry in the dictionary will have the form:
	 * <?xml version="1.0" encoding="UTF-8"?>
	 *	<synonym>
	 *		<token canonical="MIRN590" species="human" UniqueID="C568221">
	 *			<variant base="miR-390"/>
	 *		</token>	
	 *	</synonym>
	 * 
	 * @throws IOException
	 * @throws XMLStreamException
	 */
	public void createFile() throws IOException, XMLStreamException {
		// define output file
		File outputFile = new File(output);
		
		// read in the XML using ReadSupplementariesFromXML
		ReadSupplementariesFromXML reader = new ReadSupplementariesFromXML();
		ListMultimap<String, String> dict = reader.readSupplXMLUsingStax(input);

		BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));

		// start dictionary
		bw.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
		bw.write("<synonym>\n");

		for (String line : dict.keySet()) {
			String token = dict.get(line).get(0);
			String[] tokenlist = token.split("[, ]");
			bw.write("\t<token canonical=\"" + tokenlist[0] + "\" species=\""
					+ tokenlist[tokenlist.length - 1] +  "\" UniqueID=\"" + line + "\">\n");
			dict.remove(line, token);

			for (String line2 : dict.get(line)) {
				String[] synonym = line2.split("[,\\s]");

				// due to the different structure of an entry there are 3 different cases
				if (3 <= synonym.length && synonym[0].equals("microRNA")) {
					bw.write("\t\t<variant base=\"" + synonym[0] + " "
							+ synonym[1] + "\"/>\n");
				}
				else if(3 >= synonym.length && synonym[0].equals("microRNA")){
					bw.write("\t\t<variant base=\"" + synonym[1] + "\"/>\n");
				}
				else if (!synonym[0].equals("microRNA")){
					bw.write("\t\t<variant base=\"" + synonym[0] + "\"/>\n");
				}
			}

			bw.write("\t</token>\n");
		}
		bw.write("</synonym>");
		bw.close();
	}
}
