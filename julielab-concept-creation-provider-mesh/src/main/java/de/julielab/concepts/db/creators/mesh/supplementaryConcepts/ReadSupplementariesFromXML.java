/**
 * 
 */
package de.julielab.concepts.db.creators.mesh.supplementaryConcepts;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * 
 * reads in the synonymes and unique ids of microRNAs from the 
 * supplementary XML file and stores them into a multimap
 * needed for MakeMiRNADictionary.java
 * 
 * @author modersohn
 *
 */
public class ReadSupplementariesFromXML {
	
	private String id;
	private String descUI;
	private List<String> name = new ArrayList<String>();;
	
	private boolean headingMappedTo = false;
	private boolean term = false;
	private boolean miRNA = false;
	
	
	public ReadSupplementariesFromXML() {
		super();
	}



	/**
	 * 
	 * search for microRNAs and their synonymes and unique ids in supplementary XML 
	 * 
	 * @param inputXML
	 * @return multimap with unique ids and synonymes of microRNAs
	 * @throws XMLStreamException
	 * @throws FileNotFoundException
	 */
	public ListMultimap<String, String> readSupplXMLUsingStax(String inputXML) throws XMLStreamException, FileNotFoundException {
		ListMultimap<String, String> token = ArrayListMultimap.create();
		
		//initialise stream
		InputStream is = new FileInputStream(inputXML);
		XMLInputFactory factory = XMLInputFactory.newInstance();
		XMLEventReader reader = factory.createXMLEventReader(is);

		// start reading
		while(reader.hasNext()){
			XMLEvent event = reader.nextEvent();
			if (event.isStartElement()) {  
				StartElement startElement = event.asStartElement();  
				String startElementName = startElement.getName().getLocalPart();  
				
				// searches for unique id
				if(startElementName.equals("SupplementalRecordUI")){
					 id = reader.nextEvent().asCharacters().getData();				 
				}
				
				// several DescriptoUIs are defined in the XML file, we only want to search for this one
				if(startElementName.equals("HeadingMappedToList")){
					headingMappedTo = true;
				}
				
				// descriptor only registered if it belongs to the one we are looking for
				// defines the entry as microRNA
				if(startElementName.equals("DescriptorUI") && headingMappedTo == true){
					descUI = reader.nextEvent().asCharacters().getData();
					if(descUI.equals("*D035683")){
						miRNA = true;
					}
					else {
						miRNA = false;
					}
				}
				
				// synonymes are only stored within the TermList
				if(startElementName.equals("TermList")){
					term = true;
				}
				
				// synonymes are only marked as string, so only if the entry is a RNA and
				// the reader is inside a termlist the names will be registered
				if(startElementName.equals("String") && term == true && miRNA == true){
					name.add(reader.nextEvent().asCharacters().getData());
				}
				
			}
			if(event.isEndElement()){
				EndElement endElement = event.asEndElement();  
                String endElementName = endElement.asEndElement().getName().getLocalPart();  
                
                // writes the id and synonymes into the map and resets the variables
                if(endElementName.equals("SupplementalRecord") && miRNA == true){  
                	for(String synonyme : name){
                		System.out.println("will combine id: " + id + " --> with name: " + synonyme);
                		token.put(id, synonyme);
 					}
                	miRNA = false;                	
                	headingMappedTo = false;
                	term = false;
                	name.clear();
                }
                if(endElementName.equals("HeadingMappedToList")){
                	headingMappedTo = false;
                }
                if(endElementName.equals("TermList")){
                	term = false;
                }
			}
		}
		return token;
	}
}
