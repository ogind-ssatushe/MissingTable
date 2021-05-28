/** 
 * 
 */
package com.opengov.financials.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.ibatis.jdbc.ScriptRunner;

public class MissingTableEntry implements DB2Constant {

	public static String Connection_Url = null;
	public static String User_Name = null;
	public static String Password = null;
	public static String Script_file_Path_location = null;

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */

	public static void main(String[] args) throws InterruptedException, IOException {

		List<String> tableFromDatbase = new ArrayList<String>();
		List<String> tableFromScript = new ArrayList<String>();

		MissingTableEntry missingTableEntry = new MissingTableEntry();
		initSetup();
		Connection connection = getDBConnection();

		// Table from the Database.
		tableFromDatbase.addAll(missingTableEntry.checkTableFromDatabase(connection));
		List<String> sortedtableFromDatbase = tableFromDatbase.stream().sorted().collect(Collectors.toList());
		writeUnicodeJavaInputList(TABLE_FRM_DB, sortedtableFromDatbase);
		System.out.println("Fetching the table from Database and writing to the file is done....");

		// Table from the Script.
		tableFromScript.addAll(missingTableEntry.readTableNamesFromScriptFileFolder());
		List<String> sortedTableFromScript = tableFromScript.stream().sorted().collect(Collectors.toList());
		writeUnicodeJavaInputList(TABLE_FRM_SCRIPT, sortedTableFromScript);
		System.out.println("Fetching the table from script and writing to the file is done....");

		// Finding the table difference between Database and Script
		List<String> differences = new ArrayList<>(
				CollectionUtils.subtract(sortedTableFromScript, sortedtableFromDatbase));
		writeUnicodeJavaInputList(MISSING_TBL_FRM_SCRIPT, differences);
		System.out
				.println("Fetching the table from script and DB and comparing it and writing to the file is done....");

		// Finding the Matched tables between Database and Script.
		List<String> tablesFoundInDB = sortedTableFromScript.stream().distinct()
				.filter(sortedtableFromDatbase::contains).collect(Collectors.toList());
		List<String> MatchedSortedTableVsScript = tablesFoundInDB.stream().sorted().collect(Collectors.toList());
		writeUnicodeJavaInputList(MATCHED_TBL_FRM_SCRIPT_DB, MatchedSortedTableVsScript);
		System.out.println("Matching table between script and DB with writing to the file is done....");

		// Query File for the missing table entry.
		if (differences.size() > 0) {
			Map<String, String> tableToQuery = missingTableEntry.readCreateTableFromScriptFileFolder();
			Set<String> queryToExecute = new LinkedHashSet<String>();
			for (Entry<String, String> tableNameWithQuery : tableToQuery.entrySet()) {
				if (differences.contains(tableNameWithQuery.getKey())) {
					queryToExecute.add(tableNameWithQuery.getValue());
				}
			}
			writeUnicodeJavaInputSet(CREATE_QUERY_FILE, queryToExecute);
			System.out.println("Creating the sql script file, which will be executed on the db....");

			List<String> fileToQuery = missingTableEntry.mapFileToScriptFromFolder();
			writeFileNameAndScript(FILENAMEWITHQUERY, fileToQuery);
			System.out.println("Creating file with script mapping completed....");

			if (CREATE_QUERY_FILE.length() > 0) {
				executeTheQuery(CREATE_QUERY_FILE);
			}
			System.out.println("Done with the executing of script file....");
		} else {
			System.out.println("Your Tables are Upto date.");
		}

	}

	private static void initSetup() {
		File directory = new File(PATH);
		if (!directory.exists()) {
			directory.mkdir();
		}
		try {
			Properties prop = new Properties();
			ClassLoader loader = Thread.currentThread().getContextClassLoader();
			InputStream inputStream = loader.getResourceAsStream("config.properties");

			prop.load(inputStream);
			// Loading the DB properties.
			Connection_Url = prop.getProperty("db.url");
			User_Name = prop.getProperty("db.user");
			Password = prop.getProperty("db.password");
			// Loading the Script Properties.
			Script_file_Path_location = prop.getProperty("db.script.location");

		} catch (IOException io) {
			io.printStackTrace();
		}
	}

	private static void executeTheQuery(String fileWithQuery) {
		try {
			PrintWriter infoWriter = null;
			PrintWriter errorWriter = null;
			Connection connection = getDBConnection();
			infoWriter = new PrintWriter(new File(INFO_LOG_FILE));
			errorWriter = new PrintWriter(new File(ERROR_LOG_FILE));
			System.out.println("Connection established......");
			ScriptRunner sr = new ScriptRunner(connection);
			Reader reader = new BufferedReader(new FileReader(fileWithQuery));
			sr.setErrorLogWriter(errorWriter);
			sr.setLogWriter(infoWriter);
			sr.setDelimiter(";");
			sr.setStopOnError(true);
			sr.runScript(reader);
			sr.setAutoCommit(true);
			sr.closeConnection();
			connection.close();
			reader.close();
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}

	}

	public static void writeUnicodeJavaInputList(String fileName, List<String> lines) {

		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
					BufferedWriter writer = new BufferedWriter(fw)) {
				for (String line : lines) {
					writer.append(line);
					writer.newLine();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void writeUnicodeJavaInputSet(String fileName, Set<String> lines) {

		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
					BufferedWriter writer = new BufferedWriter(fw)) {
				for (String line : lines) {
					writer.append(line);
					writer.newLine();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void writeUnicodeJavaInputHashMap(String fileName, List<String> lines) {

		ExecutorService executorService = Executors.newFixedThreadPool(50);
		executorService.submit(() -> {
			try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
					BufferedWriter writer = new BufferedWriter(fw)) {
				for (String line : lines) {
					writer.append(line);
					writer.newLine();
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static void writeFileNameAndScript(String fileName, List<String> fileToQuery) {

		ExecutorService executorService = Executors.newFixedThreadPool(50);
		executorService.submit(() -> {

			try (FileWriter fw = new FileWriter(new File(fileName), StandardCharsets.UTF_8);
					BufferedWriter writer = new BufferedWriter(fw)) {
				for (String listEntry : fileToQuery) {
					writer.append(listEntry.toString());
					writer.newLine();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

	public List<String> readTableNamesFromScriptFileFolder() {

		StringBuffer sb = new StringBuffer();
		List<String> listOfTables = new ArrayList<String>();
		Set<String> uniqueTables = new LinkedHashSet<String>();

		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try {
				File directoryPath = new File(Script_file_Path_location);
				File filesList[] = directoryPath.listFiles();
				for (File file : filesList) {
					Scanner sc = new Scanner(new FileReader(file));
					if (sb.length() > 0)
						sb.delete(0, sb.length());
					while (sc.hasNextLine()) {
						String line = sc.nextLine();
						sb.append(line.trim());
					}
					String[] sqlQueries = sb.toString().split(";");
					for (int i = 0; i < sqlQueries.length; i++) {
						if (!sqlQueries[i].trim().equals("")) {
							String createStmtOnly = sqlQueries[i];
							if (createStmtOnly.startsWith(CREATE_STMT))
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
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		for (String table : uniqueTables) {
			listOfTables.add(table);
		}
		return listOfTables;
	}

	public Map<String, String> readCreateTableFromScriptFileFolder() {

		StringBuffer sb = new StringBuffer();
		Map<String, String> mapOfTableQuery = new HashMap<String, String>();

		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {

			try {
				File directoryPath = new File(Script_file_Path_location);
				File filesList[] = directoryPath.listFiles();
				for (File file : filesList) {
					Scanner sc = new Scanner(new FileReader(file));
					if (sb.length() > 0)
						sb.delete(0, sb.length());
					while (sc.hasNextLine()) {
						String line = sc.nextLine();
						sb.append(line.trim());
					}
					String[] sqlQueries = sb.toString().split(";");
					String listOfQueries = new String();
					for (int i = 0; i < sqlQueries.length; i++) {
						String createStmtOnly = sqlQueries[i];
						if (createStmtOnly.startsWith(CREATE_STMT))
							listOfQueries = sqlQueries[i] + ";";
						if (!sqlQueries[i].trim().equals("")) {
							if (createStmtOnly.startsWith(CREATE_STMT)) {
								List<String> listOfTables = new ArrayList<String>();
								listOfTables.addAll(new TableNameParser(listOfQueries).tables());
								for (String tableName : listOfTables) {
									mapOfTableQuery.put(tableName, listOfQueries);
								}
							}
						}
					}

				}
			} catch (IOException e) {
				System.out.println("There was a problem: " + e);
				e.printStackTrace();
			}

		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return mapOfTableQuery;
	}

	public List<String> mapFileToScriptFromFolder() throws InterruptedException {

		StringBuffer sb = new StringBuffer();
		List<String> fileNamewithQueries = new LinkedList<String>();
		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try {
				File directoryPath = new File(Script_file_Path_location);
				File filesList[] = directoryPath.listFiles();
				for (File file : filesList) {
					Scanner sc = new Scanner(new FileReader(file));
					String fileName = file.getName();
					if (sb.length() > 0)
						sb.delete(0, sb.length());
					while (sc.hasNextLine()) {
						String line = sc.nextLine();
						sb.append(line.trim());
					}
					String[] sqlQueries = sb.toString().split(";");
					Set<String> listOfQueries = new HashSet<String>();
					for (int i = 0; i < sqlQueries.length; i++) {
						String createStmtOnly = sqlQueries[i];
						if (createStmtOnly.startsWith(CREATE_STMT))
							listOfQueries.add(fileName + sqlQueries[i] + ";");
					}
					for (String listquery : listOfQueries) {
						fileNamewithQueries.add(listquery);
					}
				}
			} catch (IOException e) {
				System.out.println("There was a problem: " + e);
				e.printStackTrace();
			}

		});
		executorService.shutdown();
		executorService.awaitTermination(60, TimeUnit.SECONDS);

		return fileNamewithQueries;
	}

	public static Connection getDBConnection() {

		Connection connection = null;
		try {
			Class.forName(DRIVER_NAME);
			connection = DriverManager.getConnection(Connection_Url, User_Name, Password);
		} catch (Exception e) {
			System.out.println(e);
		}
		return connection;
	}

	public List<String> checkTableFromDatabase(Connection con) {

		List<String> listofTableNames = new ArrayList<String>();
		String query = "select * from sysibm.systables WHERE CREATOR like '%DB2%' AND DEFINERTYPE = 'U'";
		ExecutorService executorService = Executors.newFixedThreadPool(30);
		executorService.submit(() -> {
			try {
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(query);
				while (rs.next()) {
					listofTableNames.add(rs.getString(1));
				}
			} catch (Exception e) {
				System.out.println(e);
			} finally {
				try {
					con.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		});
		executorService.shutdown();
		try {
			executorService.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return listofTableNames;
	}
}
