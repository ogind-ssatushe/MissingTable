/** 
 * 
 */
package com.opengov.financials.utility;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;

public class MissingTableEntry {

	/**
	 * @param args
	 * @throws InterruptedException
	 */

	public static void main(String[] args) throws InterruptedException {

		String fileName = "c:\\temp\\MissingTableFromScript.txt";
		String filetableFromDatbase = "c:\\temp\\TablefromDB.txt";
		String filetableFromScript = "c:\\temp\\TablefromScript.txt";
		String fileMatchedTables = "c:\\temp\\MatchedTablesfromScript.txt";

		List<String> tableFromDatbase = new ArrayList<String>();
		List<String> tableFromScript = new ArrayList<String>();

		MissingTableEntry missingTableEntry = new MissingTableEntry();

		Connection connection = missingTableEntry.initDataBaseConnection();
		tableFromDatbase.addAll(missingTableEntry.checkTableFromDatabase(connection));
		writeUnicodeJavaInputList(filetableFromDatbase, tableFromDatbase);

		tableFromScript.addAll(missingTableEntry.readScriptFileFromFolder());
		writeUnicodeJavaInputList(filetableFromScript, tableFromScript);

		List<String> sortedTableFromScript = tableFromScript.stream().sorted().collect(Collectors.toList());
		List<String> sortedtableFromDatbase = tableFromDatbase.stream().sorted().collect(Collectors.toList());

		List<String> differences = new ArrayList<>(
				CollectionUtils.subtract(sortedTableFromScript, sortedtableFromDatbase));
		writeUnicodeJavaInputList(fileName, differences);
		
		Set<String> tablesFoundInDB = sortedTableFromScript.stream()
				  .distinct()
				  .filter(sortedtableFromDatbase::contains)
				  .collect(Collectors.toSet());
		
		writeUnicodeJavaInputSet(fileMatchedTables, tablesFoundInDB);
	}

	public static void writeUnicodeJavaInputList(String fileName, List<String> lines) {

		try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
				BufferedWriter writer = new BufferedWriter(fw)) {
			for (String line : lines) {
				writer.append(line);
				writer.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}
	
	public static void writeUnicodeJavaInputSet(String fileName, Set<String> lines) {

		try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
				BufferedWriter writer = new BufferedWriter(fw)) {
			for (String line : lines) {
				writer.append(line);
				writer.newLine();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public List<String> readScriptFileFromFolder() throws InterruptedException {

		StringBuffer sb = new StringBuffer();
		List<String> listOfTables = new ArrayList<String>();
		Set<String> uniqueTables = new HashSet<String>();

		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try {
				File directoryPath = new File("E:\\\\stw\\\\stw_java\\\\scripts\\\\");
				File filesList[] = directoryPath.listFiles();
				for (File file : filesList) {
					Scanner sc = new Scanner(new FileReader(file));
					while (sc.hasNextLine()) {
						String line = sc.nextLine();
						sb.append(line);
					}
					String[] sqlQueries = sb.toString().split(";");
					for (int i = 0; i < sqlQueries.length; i++) {
						if (!sqlQueries[i].trim().equals("")) {
							uniqueTables.addAll(new TableNameParser(sqlQueries[i]).tables());
						}
					}
				}
			} catch (IOException e) {
				System.out.println("There was a problem: " + e);
				e.printStackTrace();
			}
		});
		executorService.shutdown();
		executorService.awaitTermination(60, TimeUnit.SECONDS);
		for (String table : uniqueTables) {
			listOfTables.add(table);
		}
		return listOfTables;
	}

	public Connection initDataBaseConnection() {
		final String driver = "com.ibm.db2.jcc.DB2Driver";
		final String connection = "jdbc:db2://localhost:50000/STWTPDB";
		final String user = "db2admin";
		final String password = "Financials1";
		Connection con = null;
		try {
			Class.forName(driver);
			con = DriverManager.getConnection(connection, user, password);
		} catch (Exception e) {
			System.out.println(e);
		}
		return con;
	}

	public List<String> checkTableFromDatabase(Connection con) {
		List<String> listofTableNames = new ArrayList<String>();
		try {
			DatabaseMetaData metaData = con.getMetaData();
			ResultSet rs = metaData.getTables(null, null, "%", null);
			while (rs.next()) {
				listofTableNames.add(rs.getString("TABLE_NAME"));
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return listofTableNames;
	}
}
