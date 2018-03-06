package de.julielab.concepts.db.creators.mesh;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.julielab.concepts.db.creators.mesh.components.Descriptor;
import de.julielab.concepts.db.creators.mesh.components.TreeVertex;
import de.julielab.concepts.db.creators.mesh.exchange.DataExporter;
import de.julielab.concepts.db.creators.mesh.exchange.DataImporter;

/**
 * Executable class that contains some sample applications for the classes developed in the mesh package.
 * 
 * @author Philipp Lucas
 */
public class Process {

	private static Logger logger = LoggerFactory.getLogger(Process.class);
	private static Properties appProps;

	/**
	 * <p>
	 * Main procedure.
	 * </p>
	 * 
	 * <p>
	 * Work flow depends on the settings in the configuration file. See that file and the source code for full details.
	 * </p>
	 * 
	 * @param args
	 *            Expected arguments are in this order:
	 *            <ol>
	 *            <li>'Path to config file'</li>
	 *            </ol>
	 */
	public static void main(String[] args) {

		// get properties from command line arguments (parses config file)
		appProps = getPropertiesFromCmdLineArgs(args);

		// do as told in the config file ...
		if (isProp("createAgingMesh")) {
			processForAgingMesh();
		}

		if (isProp("create2008SemedicoMesh")) {
			create2008SemedicoMesh();
		}

		if (isProp("createSemedicoMesh")) {
			createSemedicoMesh();
		}

	}

	/**
	 * Parses properties for this run from command line arguments, by using the first command line argument as a file
	 * path to a config file.
	 * 
	 * @param args
	 *            command line arguments.
	 * @return Properties read from config file.
	 */
	public static Properties getPropertiesFromCmdLineArgs(String[] args) {
		String usage = "You need to specify exactly one argument: \n 'Path to config file'";

		logger.info("# Checking command line arguments ... ");

		if (args.length != 1) {
			logger.error(usage);
			System.exit(1);
		}

		Properties props = new Properties();
		try {
			logger.info("# Loading config file '" + args[0] + "'... ");
			props.load(new FileReader(args[0]));
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			System.exit(1);
		} catch (IOException e) {
			logger.error(e.getMessage());
			System.exit(1);
		}

		logger.info("# ... both done.");
		return props;
	}

	/**
	 * <p>
	 * Imports old UD MeSH data, compares them with the data provided in <code>mesh</code>, and saves all modifications
	 * between them in a set of files and finally applies the modifications in order to create the 2008 semedico mesh p>
	 * 
	 * @param mesh
	 *            original MeSH data.
	 */
	private static void create2008SemedicoMesh() {
		try {
			// import old intermediate MeSH XML data for semedico
			Tree udMesh2008 = new Tree("UD-MeSH 2008");
			DataImporter.fromUserDefinedMeshXml(getProp("udMesh2008FilePath"), udMesh2008);
			udMesh2008.verifyIntegrity();

			// import MeSH 2008 data
			Tree mesh2008 = new Tree("MeSH 2008");
			DataImporter.fromOriginalMeshXml(getProp("mesh2008FilePath"), mesh2008);
			mesh2008.verifyIntegrity();

			// determine modifications
			TreeComparatorUD comparator = new TreeComparatorUD(mesh2008, udMesh2008);
			comparator.determineModifications();
			mesh2008.verifyIntegrity();

			// export modifications
			if (isProp("sem2008ExportMods")) {
				comparator.saveModificationsToFiles(getProp("sem2008ExportModsBaseFilePath"));
				udMesh2008.verifyIntegrity();
				mesh2008.verifyIntegrity();
			}

			if (isProp("sem2008ApplyModsAndExport")) {
				// apply modifications to original MeSH
				TreeModificator modifier = new TreeModificator(mesh2008);
				modifier.putModification(comparator);
				modifier.applyAll(true);

				// // get modifications FROM FILES
				// DescAdditions descAdds =
				// ModificationImporter.importDescAdditions(getProp("compareExportModsBaseFilePath") + "_newDescs.xml");
				// VertexMovings vertexMovings =
				// ModificationImporter.importVertexMovings(getProp("compareExportModsBaseFilePath") +
				// "_movedVertices.csv");
				// VertexDeletions vertexDeletions =
				// ModificationImporter.importVertexDeletions(getProp("compareExportModsBaseFilePath") +
				// "_deletedVertices.csv");

				DataExporter.toOwnXml(mesh2008, getProp("sem2008ExportCreatedXMLFilePath"));
				DataExporter.toSIF(mesh2008, getProp("sem2008ExportCreatedSIFFilePath"));
			}

			DataExporter.toOwnXml(udMesh2008, getProp("sem2008ExportXMLFilePath"));
			DataExporter.toSIF(udMesh2008, getProp("sem2008ExportSIFFilePath"));

			// run interactive menu
			// Set<Tree> tmpSet = new HashSet<Tree>();
			// tmpSet.add(udMesh);
			// tmpSet.add(mesh);
			// interactiveMenu(tmpSet);

			logger.info("# good bye ");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * Creates the current Semedico-MeSH based on Semedico-MeSH 2008 and MeSH 2008 and current MeSH "Current" depends on
	 * the ini-file
	 */
	private static void createSemedicoMesh() {
		try {
			// parse original mesh (as of 2008)
			Tree mesh2008 = new Tree("MeSH-2008");
			DataImporter.fromOriginalMeshXml(getProp("mesh2008FilePath"), mesh2008);
			mesh2008.verifyIntegrity();

			// parse ud-mesh (as of 2008)
			Tree udMesh2008 = new Tree("ud-MeSH-2008");
			DataImporter.fromUserDefinedMeshXml(getProp("udMesh2008FilePath"), udMesh2008);
			udMesh2008.verifyIntegrity();

			// for (Descriptor d : udMesh2008.childDescriptorsOf(udMesh2008.getRootDesc())) {
			// System.out.println(d.getName());
			// for (Descriptor c : udMesh2008.childDescriptorsOf(d))
			// System.out.println("\t" + c);
			// }

			// determine modifications
			TreeComparatorUD mods4semedico2008 = new TreeComparatorUD(mesh2008, udMesh2008);
			mods4semedico2008.determineModifications();
			mesh2008.verifyIntegrity();

			// parse current mesh
			Tree meshCurrent = new Tree("MeSH-current");
			DataImporter.fromOriginalMeshXml(getProp("meshCurrentFilePath"), meshCurrent);
			meshCurrent.verifyIntegrity();

			// update modifications
			TreeModificationMerger merger = new TreeModificationMerger(mesh2008, meshCurrent, mods4semedico2008);
			TreeModificator mods4semedicoCurrent = merger.merge();

			// apply modifications on current mesh -> create current semedico mesh
			mods4semedicoCurrent.applyAll(true);

			// for (Descriptor d : meshCurrent.childDescriptorsOf(meshCurrent.getRootDesc())) {
			// System.out.println(d.getName());
			// for (Descriptor c : meshCurrent.childDescriptorsOf(d))
			// System.out.println("\t" + c);
			// }

			// Added by Erik in 2014. Some top-level nodes (ca. 450) were no facet nodes but homeless terms that
			// didn't
			// find connection. The best way would have been to find the cause, but for time reasons this class just
			// tries to repair this.
			ModifiedTreeCleaner modifiedTreeCleaner = new ModifiedTreeCleaner(meshCurrent, udMesh2008);
			modifiedTreeCleaner.clean();

			// export modifications for current semedico mesh
			mods4semedicoCurrent.saveModificationsToFiles(getProp("semedicoCurrentExportModsBaseFilePath"));

			// export current semedico mesh
			DataExporter.toOwnXml(meshCurrent, getProp("semedicoCurrentExportXMLFilePath"));
			DataExporter.toSIF(meshCurrent, getProp("semedicoCurrentExportSIFFilePath"));

			mesh2008.printInfo(System.out);
			meshCurrent.printInfo(System.out);

			// be happy.
			logger.info("# good bye!");
		} catch (Exception e) {
			logger.error(e.getMessage());
			e.printStackTrace();
		}
	}

	/**
	 * Creates the aging mesh.
	 * 
	 * @param data
	 */
	private static void processForAgingMesh() {
		try {
			logger.info("## Creating Aging-MeSH  ... ");

			Tree data = new Tree("Aging-MeSH");
			DataImporter.fromOriginalMeshXml(getProp("agingMeshFilePath"), data);
			data.verifyIntegrity();

			// for Aging MeSH: add some new descriptors
			DataImporter.fromOriginalMeshXml(getProp("agingAdditionalTermsFilePath"), data);

			// for Aging MeSH: filter data
			logger.info("# Filter data using whitelist '" + getProp("agingUIWhiteListFilePath") + "' ... ");
			TreeFilter filter = new TreeFilter(data, TreeFilter.THROW, TreeFilter.THROW);
			filter.maskDescByUIFile(getProp("agingUIWhiteListFilePath"), true, true);
			filter.apply();
			data.verifyIntegrity();
			logger.info("# ... done filtering.");

			DataExporter.toOwnXml(data, getProp("agingExportFilePath"));

			logger.info("## ... done creating Aging-MeSH.");
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * A small interactive menu to get information about Tree instances
	 * 
	 * @param data
	 *            A set of Tree objects.
	 */
	@SuppressWarnings("unused")
	private static void interactiveMenu(Set<Tree> data) {
		try {
			if (data == null || data.isEmpty()) {
				logger.error("set of data is empty - aborting.");
				return;
			}
			String selection = "";
			Tree curData = data.iterator().next();

			InputStreamReader isr = new InputStreamReader(System.in);
			BufferedReader br = new BufferedReader(isr);

			do {
				System.out.println("What do you want to do?");
				System.out.println("s ... select data set");
				System.out.println("p ... print general information about current data set");
				System.out.println("d ... print information about a descriptor");
				System.out.println("v ... print information about a tree vertex");
				System.out.println("x ... exit \n");

				selection = br.readLine();

				if (selection.equals("s")) {
					System.out.println("There are : ");
					int cnt = 1;
					for (Tree d : data) {
						System.out.println("(" + cnt + ") " + d.getName());
						cnt++;
					}
					System.out.print("Select the number: ");
					try {
						int number = Integer.parseInt(br.readLine());
						if (number > data.size()) {
							throw new NumberFormatException();
						}
						cnt = 1;
						for (Tree d : data) {
							curData = d;
							if (cnt == number) {
								break;
							}
							cnt++;
						}
					} catch (NumberFormatException e) {
						System.err.println("You didn't type a valid number.");
					}

				} else if (selection.equals("p")) {
					curData.printInfo(System.out);

				} else if (selection.equals("d")) {
					System.out.print("Type UI or name of descriptor to display: ");
					String nameUi = br.readLine();

					if (curData.hasDescriptorByName(nameUi)) {
						Descriptor d = curData.getDescriptorByName(nameUi);
						System.out.println(d.tofullString(curData));
					} else if (curData.hasDescriptorByUi(nameUi)) {
						Descriptor d = curData.getDescriptorByUi(nameUi);
						System.out.println(d.tofullString(curData));

					} else {
						System.out.println(" '" + nameUi + "' is not a valid UI or name of a descriptor.");
					}

				} else if (selection.equals("v")) {
					System.out.print("Type name of tree vertex to display: ");
					String name = br.readLine();

					if (curData.hasVertex(name)) {
						TreeVertex v = curData.getVertex(name);
						System.out.println(v.toFullString(curData));
					} else {
						System.out.println(" '" + name + "' is not a valid name of a tree vertex.");
					}
				}

			} while (!selection.equals("x"));
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * @param key
	 *            Name of the property to get.
	 * @return Returns the value of property with key <code> key </code>
	 */
	public static String getProp(String key) {
		return appProps.getProperty(key);
	}

	/**
	 * @param key
	 *            Name of a property.
	 * @return Returns true if the value of the property with key = <code>key</code> equals the string true. False
	 *         otherwise (also if it is unset).
	 */
	public static boolean isProp(String key) {
		String value = getProp(key);
		if (value == null || !value.equals("true")) {
			return false;
		} else {
			return true;
		}
	}

}
