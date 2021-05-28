/**
 * 
 */
package com.opengov.financials.utility;

import java.util.Arrays;
import java.util.List;

/**
 * @author db2admin
 *
 */
public interface DB2Constant {
	public static final String PATH = "C:\\temp\\";
	public static final String CREATE_QUERY_FILE = "c:\\temp\\FileWithCreateQuery.sql";
	public static final String NON_CREATE_QUERY_FILE = "c:\\temp\\FileWithNonCreateQuery.sql";
	public static final String MISSING_TBL_FRM_SCRIPT = "c:\\temp\\MissingTableFromScript.txt";
	public static final String TABLE_FRM_DB = "c:\\temp\\TablefromDB.txt";
	public static final String TABLE_FRM_SCRIPT = "c:\\temp\\TablefromScript.txt";
	public static final String MATCHED_TBL_FRM_SCRIPT_DB = "c:\\temp\\MatchedTablesfromScript.txt";
	public static final String INFO_LOG_FILE = "c:\\\\temp\\\\infolog.txt";
	public static final String ERROR_LOG_FILE = "c:\\\\temp\\\\errorlog.txt";
	public static final String FILENAMEWITHQUERY = "c:\\temp\\FileNameWithQuery.txt";

	public static final int NO_INDEX = -1;
	public static final String SPACE = " ";
	public static final String REGEX_SPACE = "\\s+";

	public static final String TOKEN_START = "/*+";
	public static final String TOKEN_END = "*/";
	public static final String TOKEN_SINGLE_LINE_COMMENT = "--";
	public static String TOKEN_NEWLINE = "\\r\\n|\\r|\\n|\\n\\r";
	public static final String TOKEN_SEMI_COLON = ";";
	public static final String TOKEN_PARAN_START = "(";
	public static final String TOKEN_COMMA = ",";
	public static final String TOKEN_SET = "set";
	public static final String TOKEN_OF = "of";
	public static final String TOKEN_DUAL = "dual";
	public static final String TOKEN_DELETE = "delete";
	public static final String TOKEN_CREATE = "create";
	public static final String TOKEN_INDEX = "index";
	public static final String TOKEN_ASTERICK = "*";

	public static final String KEYWORD_JOIN = "join";
	public static final String KEYWORD_INTO = "into";
	public static final String KEYWORD_TABLE = "table";
	public static final String KEYWORD_FROM = "from";
	public static final String KEYWORD_USING = "using";
	public static final String KEYWORD_UPDATE = "update";

	public static final List<String> concerned = Arrays.asList(KEYWORD_TABLE, KEYWORD_INTO, KEYWORD_JOIN, KEYWORD_USING,
			KEYWORD_UPDATE);
	public static final List<String> ignored = Arrays.asList(TOKEN_PARAN_START, TOKEN_SET, TOKEN_OF, TOKEN_DUAL);

	public final String DRIVER_NAME = "com.ibm.db2.jcc.DB2Driver";

	public static final String CREATE_STMT = "CREATE TABLE";

	public static final String CREATE_INDEX_STMT = "CREATE INDEX";
	
	public static final String CREATE_UNIQUE_INDEX_STMT = "CREATE UNIQUE";

}
