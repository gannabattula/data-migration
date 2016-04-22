/**
 * 
 */
package com.ows.cep.dbmigration;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;



/**
 * @author rgannaba
 *
 */
public class SqlServierDataScriptFiles {

	/**
	 * 
	 */
	public SqlServierDataScriptFiles() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws Exception{
		
		
		String propertiesFile = System.getProperty("propfile");
		if(propertiesFile == null || propertiesFile.isEmpty()){
			propertiesFile = args[0];
		}
		System.out.println("File Name  " + propertiesFile );
		
		Properties props = loadPropFile(propertiesFile);
		
		if(props == null || props.isEmpty()){
			throw new Exception("Please provide propertis file as a input String");
		}
		// Create a variable for the connection string.
		//http://stackoverflow.com/questions/18841744/jdbc-connection-failed-error-tcp-ip-connection-to-host-failed  follow this to set port
		//https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774 -- to download driver
		
/*		select * from syscolumns where id=1767677345  order by name;

		select * from sysobjects;

		SELECT * FROM sysobjects sobjects WHERE sobjects.xtype = 'U' order by name; */
		/*
		bit
		char
		date
		datetime
		decimal
		image
		int
		nchar
		ntext
		numeric
		nvarchar
		sysname
		uniqueidentifier*/
		
		
		//String connectionUrl = "jdbc:sqlserver://localhost:1434;databaseName=AdventureWorks;integratedSecurity=true;";
		String connectionUrl = props.getProperty("connectionUrl");
		String sqlserverUser = props.getProperty("sqlserverUser");
		String sqlPassword =props.getProperty("sqlPassword");
		
		/*String connectionUrl = "jdbc:sqlserver://MT-35:1433;databaseName=OWS_MCD_PreProd";
		String sqlserverUser = "sa";
		String sqlPassword ="Passw0rd@1";*/
		
		
		String dataMigrationFolder =props.getProperty("dataMigrationFolder");;
		String sqlloaderFileDir = dataMigrationFolder;
		String sqlloaderFileName = "sqlloader.bat";
		String dataFileFolder=dataMigrationFolder + "\\data";
		String logfilesDir = dataMigrationFolder + "\\log";
		String controlFilesfolderDir = dataMigrationFolder + "\\control";
		String badFileFolderDir = dataMigrationFolder + "\\bad";
		
		String cepUser = "CEP25";
		String cepPassword = "CEP25";
		String cepConnectionString = "jdbc:oracle:thin:@//localhost:1521/orcl";
		String env="windows";

		String pathNameSqlloader= sqlloaderFileDir +  File.separatorChar + sqlloaderFileName;

		//generate data files
		generateDataFiles(connectionUrl, sqlserverUser, sqlPassword,
				sqlloaderFileDir, dataFileFolder, logfilesDir,
				controlFilesfolderDir, badFileFolderDir,dataMigrationFolder);
		
		
		//genearete sqlloader file
		
		generateSqlloader(props.getProperty("sqlloaderTemplate"),props.getProperty("cepUserIdString"), dataMigrationFolder , props.getProperty("env") );
		
		//copy control files 
		
		
		File file = new File("dummy.txt");
		String path = file.getAbsolutePath();
		
		String[] str = path.split("dummy.txt");

		String controlPath =  str[0] + "control";
		System.out.println("controlfile path " + controlPath);
		//copyControlfilesToControlFolder("/ctrl" , controlFilesfolderDir);
		InputStream stream = SqlServierDataScriptFiles.class.getResourceAsStream("/resources/control.zip");	
		
		unzip(stream, controlFilesfolderDir);
		
		
	}

	private static void generateDataFiles(String connectionUrl,
			String sqlserverUser, String sqlPassword, String sqlloaderFileDir,
			String dataFileFolder, String logfilesDir,
			String controlFilesfolderDir, String badFileFolderDir, String dataMigrationFolder)
			throws ClassNotFoundException, SQLException, IOException {
		
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");	
		Connection conn = DriverManager.getConnection(connectionUrl,sqlserverUser, sqlPassword);
		
		Statement sta = conn.createStatement();
		String Sql = "SELECT * FROM sysobjects sobjects WHERE sobjects.xtype = 'U' order by name";
		ResultSet rs = sta.executeQuery(Sql);
		
		String dataModifledFileName = dataMigrationFolder + File.separatorChar + "dataModifiedTableandColumn.sql";
		
		delete(new File(dataFileFolder) );
		createFolder(sqlloaderFileDir);
		createFolder(dataFileFolder);
		createFolder(controlFilesfolderDir);
		createFolder(logfilesDir);
		createFolder(badFileFolderDir);
		delete(new File(dataModifledFileName) );
		
		
		PrintWriter dataAlterationFile = getFilewriterByCreatingAFile(dataMigrationFolder, dataModifledFileName);
		
		while (rs.next()) {
			String tableName =rs.getString("name");
			String fileName = tableName.toUpperCase() + ".data";
			String dataFileName= dataFileFolder +  File.separatorChar + fileName;

			Set<String> dataModifiedColumns = new HashSet<String>();
			PrintWriter pw = getFilewriterByCreatingAFile(dataFileFolder, dataFileName);
	          //This will add a new line to the file content
	    	  //pw.println("");
	         //get control data 
	    	
			
		       
			
			Statement staColum = conn.createStatement();
			//String sqlColm = "select   syscolumns.name column_name ,systypes.name datatype , syscolumns.length length from syscolumns inner join systypes on syscolumns.xtype = systypes.xtype where syscolumns.id = 2046630334 order by colid";
			String sqlColm = "select   syscolumns.name column_name ,systypes.name datatype , syscolumns.length length from syscolumns inner join systypes on syscolumns.xtype = systypes.xtype and systypes.status=0 where syscolumns.id = " +  rs.getString("id")  + " order by syscolumns.colid";
			
			
			ResultSet rsCol = staColum.executeQuery(sqlColm);
			//ResultSetMetaData rsmeta = rsCol.getMetaData();
			Statement staColumData = conn.createStatement();
			//String sqlColmData = "select * from  emails where id<20";
			String sqlColmData = "select * from  " +  rs.getString("name");
			ResultSet rsColdata = staColumData.executeQuery(sqlColmData);
			ResultSetMetaData rsmeta = rsColdata.getMetaData();
			if(rsColdata.next()){
				Map<String, String> colMap = new HashMap<String, String>();
				Map<String, String> colMapDataType = new HashMap<String, String>();
				Map<String, String> colMapDataLength = new HashMap<String, String>();
				int columnSize=1;
				
				StringBuffer headString = new StringBuffer();
				headString.append("");
				while(rsCol.next()){
					//	System.out.print(rsCol.getString("name"));
					 /*  if(columnSize>1){
						   headString.append("~");
					   }
					*/ 
					  headString.append(rsCol.getString("column_name"));
						
						
						String key = "col" + columnSize;
						colMap.put(key, rsCol.getString("column_name"));
						colMapDataType.put(key, rsCol.getString("datatype"));
						colMapDataLength.put(key, rsCol.getString("length"));
						headString.append("~");
						columnSize=columnSize+1;
					}
				//get contorl file
			//	String controlHeader = getControlHeader(colMap, colMapDataType,colMapDataLength, columnSize, tableName);
				//pw.println(controlHeader);
				//pw.println(System.lineSeparator());
				
				//pw.println("begindata");
				
				pw.println(headString.toString());
				//headString.append("\n");
				
				StringBuffer recordStringFirst = getRecordString(rsColdata,
						colMap, colMapDataType, columnSize, dataModifiedColumns);
				String recordStringFirstRow = recordStringFirst.toString();
				//if(tableName.equalsIgnoreCase("OIMXMLSNAPSHOT")){
				//	recordStringFirstRow = recordStringFirstRow.replaceAll("\r\n", "");
				//}
				
				pw.println(recordStringFirstRow);
		    	
				
				while(rsColdata.next()){
					StringBuffer recordString = getRecordString(rsColdata,
							colMap, colMapDataType, columnSize,dataModifiedColumns);
					String recordStringNext = recordString.toString();
					//if(tableName.equalsIgnoreCase("OIMXMLSNAPSHOT")){
						//recordStringNext= recordStringNext.replaceAll("\r\n", "");
					//}
					pw.println(recordStringNext);
			    	 
			    	 
				} //columns loop4
			}else{
				StringBuffer headString = new StringBuffer();
				int columnSize =1;
				while(rsCol.next()){
					/*if(columnSize > 1){
						headString.append("~");
					}*/
					//	System.out.print(rsCol.getString("name"));
					headString.append(rsCol.getString("column_name"));
					headString.append("~");
					}
				//headString.append("~");
				 pw.println(headString.toString());
				 //System.out.println("Missing data file is done for table " + tableName);
			}  
			
			 pw.close();
			 System.out.println("data file is done for table " + tableName);
			 
			//write the table name and columns with , seperation 
			 if(!dataModifiedColumns.isEmpty()){
				 // get primary key and prepare the update statment for both type of massage..
				 
				 String primarykey= 	"SELECT Col.Column_Name from INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab, INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col"
							+ " WHERE   Col.Constraint_Name = Tab.Constraint_Name  AND Col.Table_Name = Tab.Table_Name AND Constraint_Type = 'PRIMARY KEY' AND "
							+ "Col.Table_Name = '" +  tableName + "'";
					PreparedStatement stmtprimaykey = conn.prepareStatement(primarykey);
					ResultSet rsprimaykey = stmtprimaykey.executeQuery();	
					List<String> pkList = new ArrayList<String>();
					while(rsprimaykey.next()){
						pkList.add(rsprimaykey.getString("column_name"));
					}
						
				
				 Iterator<String> itr = dataModifiedColumns.iterator();
				 
				 while(itr.hasNext()){
					 String columnName = itr.next();
					 StringBuffer updateStatment = getUpdateStringForDataMassage(
							tableName, pkList, columnName,  "@#@",  "\\r\\n");
					//write to file
						//dataAlterationFile.println(updateStatment.toString());
						 updateStatment = getUpdateStringForDataMassage(
									tableName, pkList, columnName,  "@&@",  "~");
							//write to file
								dataAlterationFile.println(updateStatment.toString());
				 }
			
							
				
			 }
				
				
		}//tables loop
		
		
		dataAlterationFile.println("commit;");
		dataAlterationFile.println("exit");
		dataAlterationFile.close();
	}

	private static StringBuffer getUpdateStringForDataMassage(String tableName,
			List<String> pkList, String columnName, String regex, String replaceString) {
		int pk =1;
		 StringBuffer updateStatment = new StringBuffer();
			// update APPERRORLOG ap set ap.errormsg_ = (select REGEXP_REPLACE(ERRORMSG_, '~#~','\r\n') errmsg from APPERRORLOG apin  WHERE REGEXP_LIKE(ERRORMSG_, '~#~') and ap.errorlogid=apin.errorlogid )   ;
		 //update ProposedItem t1 set t1.QualityContactEmail =  REGEXP_REPLACE(t1.QualityContactEmail, '@&@','~') where t1.ID=(select ID from   ProposedItem t2  WHERE REGEXP_LIKE(t2.QualityContactEmail, '@&@') and t1.Id=t2.id)
		 updateStatment.append("update " );
		 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(tableName));
		 updateStatment.append(" t1 set " );
		
		 updateStatment.append("t1.");
		 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(columnName));
		 
		 updateStatment.append(" = ");
		 updateStatment.append(" REGEXP_REPLACE(t1.");
		 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(columnName));
		 updateStatment.append(", '" + regex + "','" + replaceString + "')" );
		 /*updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(tableName));
		 updateStatment.append(" t2  WHERE REGEXP_LIKE(t2." );
		 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(columnName));
		 updateStatment.append(", '" + regex + "') and ");
		 */
		 updateStatment.append(" where ");
		 for(String pkStr : pkList){
			 if(pk>1){
				 updateStatment.append(" and ");
			 }
			 updateStatment.append(" t1.");
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(pkStr));
			 updateStatment.append(" = ");
			 updateStatment.append(" ( select ");
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(pkStr));
			 updateStatment.append(" from ");
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(tableName));
			 updateStatment.append(" t2  WHERE REGEXP_LIKE(t2." );
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(columnName));
			 updateStatment.append(", '" + regex + "') and ");
			 updateStatment.append(" t1.");
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(pkStr));
			 updateStatment.append(" = ");
			 updateStatment.append(" t2.");
			 updateStatment.append(getTableOrColumnDifferencesBetweenOracleAndSql(pkStr));
			
			 pk++;
		 }
		 updateStatment.append(");");
		return updateStatment;
	}
	
	private static String getTableOrColumnDifferencesBetweenOracleAndSql(String name){
		
		InputStream stream = SqlServierDataScriptFiles.class.getResourceAsStream("/resources/TableNameDifferences.properties");	
		Properties properties = new Properties();
		if(properties.isEmpty()){
			try {
				properties.load(stream);
			} catch (IOException e) {
				System.out.println("Unable to load CEPErrors properties" + e.getMessage());
			}
		}
		String str = properties.getProperty(name.toUpperCase());
		String returnVal= name;
		if(str != null && !str.isEmpty() ){
			returnVal = str;
		}
		return returnVal;
	}
	
	private static void copyControlfilesToControlFolder(String sourcefolder, String distnationfolder) throws IOException{
		
	  	
		copyDirectory(new File(sourcefolder), new File(distnationfolder));
	}
	private static void generateSqlloader(String sqlloaderTemplate , String cepUserIdString, String dataMigrationFolder, String env ) throws FileNotFoundException{
		
		try
        {
			
			  
		
				sqlloaderTemplate = "/resources/sqlloaderTemplate.template";
				String fileWindows = dataMigrationFolder + File.separatorChar + "sqlloaderexecutable.bat";
				String fileLinux = dataMigrationFolder + File.separatorChar + "sqlloaderexecutable.sh";
				
		InputStream stream = SqlServierDataScriptFiles.class.getResourceAsStream(sqlloaderTemplate);	
		
        //File file = new File(sqlloaderTemplate);
       // System.out.println("file path " + file.getAbsolutePath()) ;
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        String line = "", oldtext = "";
        while((line = reader.readLine()) != null)
            {
            oldtext += line + "\r\n";
        }
        reader.close();
            
    	String cepUserIdStringwindows = "";
    	String cepUserIdStringlinux = "";

        
        if(cepUserIdString.isEmpty()){
        	cepUserIdStringwindows = "%CEP_CONNECTION_STRING%";
        	cepUserIdStringlinux ="$CEP_CONNECTION_STRING";
        }else{
        	cepUserIdStringwindows = cepUserIdString;
        	cepUserIdStringlinux = cepUserIdString;

        }
        
        String dataFileFolder= "%DATA_MIGRATION_HOME%" + "\\data\\";
		String logfilesDir = "%DATA_MIGRATION_HOME%" + "\\log\\";
		String controlFilesfolderDir ="%DATA_MIGRATION_HOME%" +  "\\control\\";;
		String badFileFolderDir = "%DATA_MIGRATION_HOME%" + "\\bad\\";
        
      
		  String dataFileFolderLinux="\\$DATA_MIGRATION_HOME" + "/data/";
			String logfilesDirLinux = "\\$DATA_MIGRATION_HOME" + "/log/";
			String controlFilesfolderDirLinux ="\\$DATA_MIGRATION_HOME" + "/control/";
			String badFileFolderDirLinux = "\\$DATA_MIGRATION_HOME" + "/bad/";
	        
			
        	
        	
            dataFileFolder = dataFileFolder.replaceAll("\\\\", "\\\\\\\\");
       		logfilesDir = logfilesDir.replaceAll("\\\\", "\\\\\\\\");
       		controlFilesfolderDir = controlFilesfolderDir.replaceAll("\\\\", "\\\\\\\\");
       		badFileFolderDir = badFileFolderDir.replaceAll("\\\\", "\\\\\\\\");
           
       		
       
        //To replace a line in a file
		String windowsText = oldtext;
		
		String sqlloaderText = windowsText.replaceAll("sqlldr", "%ORACLE_BIN_PATH%" + "\\\\sqlldr");
        String userReplacement = sqlloaderText.replaceAll("cepUserIdString_", cepUserIdStringwindows);
        String controlReplacement = userReplacement.replaceAll("controlFolderString_", controlFilesfolderDir);
        String dataReplacement = controlReplacement.replaceAll("dataFolderString_", dataFileFolder);
        String logReplacement = dataReplacement.replaceAll("logFolderString_", logfilesDir);
        String finalString = logReplacement.replaceAll("badFolderString_", badFileFolderDir);
      
        
        FileWriter writer = new FileWriter(fileWindows);
        writer.write(finalString);
        writer.close();
        
        String LinuxText = oldtext;
        String sqlloaderTextLinux = LinuxText.replaceAll("sqlldr", "\\$ORACLE_BIN_PATH/sqlldr");
        String userReplacementlinux = sqlloaderTextLinux.replaceAll("cepUserIdString_", cepUserIdStringlinux);
        String controlReplacementLinux = userReplacementlinux.replaceAll("controlFolderString_", controlFilesfolderDirLinux);
        String dataReplacementLinux = controlReplacementLinux.replaceAll("dataFolderString_", dataFileFolderLinux);
        String logReplacementLinux = dataReplacementLinux.replaceAll("logFolderString_", logfilesDirLinux);
        String finalStringLinux = logReplacementLinux.replaceAll("badFolderString_", badFileFolderDirLinux);
      

        FileWriter writerLinux = new FileWriter(fileLinux);
        writerLinux.write(finalStringLinux);
        writerLinux.close();
        
        System.out.println("sqlloderfile generated at " +  fileLinux);
        
    }
    catch (IOException ioe)
        {
        ioe.printStackTrace();
    }
	}

	/**
	 * @param folderDir
	 * @param pathName
	 * @return
	 * @throws IOException
	 */
	private static PrintWriter getFilewriterByCreatingAFile(String folderDir,
			String pathName) throws IOException {
		File fileDir = new File(folderDir);
		if(!fileDir.exists()){
			fileDir.mkdirs();
		}
		
		
		File filePath = new File(pathName);
		if(!filePath.exists()){
			filePath.createNewFile();
		}
		
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter( new FileOutputStream(filePath), "UTF-8"));
		
		/*
		  FileWriter fw = new FileWriter(filePath,true);
		  BufferedWriter bw = new BufferedWriter(fw);*/
		  PrintWriter pw = new PrintWriter(bw);
		return pw;
	}

	/**
	 * @param rsColdata
	 * @param colMap
	 * @param colMapDataType
	 * @param columnSize
	 * @return
	 * @throws SQLException
	 */
	private static StringBuffer getRecordString(ResultSet rsColdata,
			Map<String, String> colMap, Map<String, String> colMapDataType,
			int columnSize, Set dataModifiedColumns) throws SQLException {
		StringBuffer recordString = new StringBuffer();
		int j=1;
		for (int i=1; i< columnSize; i++) {
		    String key = "col" + i;
		  //  if(rsmeta.)
		    String dataTypeString = colMapDataType.get(key);
		  
		   /* if(j > 1){
				recordString.append("~");
			}*/
		    if(dataTypeString.equalsIgnoreCase("char") || dataTypeString.equalsIgnoreCase("image") ||  dataTypeString.equalsIgnoreCase("nchar") 
		    		||  dataTypeString.equalsIgnoreCase("ntext") || dataTypeString.equalsIgnoreCase("nvarchar") || dataTypeString.equalsIgnoreCase("varchar") || dataTypeString.equalsIgnoreCase("sysname") || dataTypeString.equalsIgnoreCase("uniqueidentifier") ){
		    	  String columnValue = rsColdata.getString(colMap.get(key));
		    	if(columnValue != null && !columnValue.equalsIgnoreCase("null")){
			    	
		    		if (columnValue.contains("\r\n") ){
		    			//String colVal = columnValue.toString();
		    			columnValue=columnValue.replaceAll("\r\n", "");
		    			//dataModifiedColumns.add(colMap.get(key));
		    			
		    		}
		    		if (columnValue.contains("~")){
		    			//String colVal = columnValue.toString();
		    				columnValue=columnValue.replaceAll("~", "@&@");
		    				dataModifiedColumns.add(colMap.get(key));
		    		}
		    		
		    		
		    		//recordString.append("'");
		    		
			    	recordString.append(columnValue);
		    		
			    	//recordString.append("'");
		    	}
		    }else if(dataTypeString.equalsIgnoreCase("bit") || dataTypeString.equalsIgnoreCase("int") || dataTypeString.equalsIgnoreCase("numeric") ){
		    	
		    	  Object columnValue = rsColdata.getObject(colMap.get(key));
		    	  if(columnValue != null && !columnValue.toString().equalsIgnoreCase("null")){
		  //  		  recordString.append("'");
		    		  recordString.append(columnValue);
		    //		  recordString.append("'");
		    	  }
		    	
		    }else if(dataTypeString.equalsIgnoreCase("date") ){
		    	  Date columnValue = rsColdata.getDate(colMap.get(key));
		    	if(columnValue != null && !columnValue.toString().equalsIgnoreCase("null")){
				    
		    		DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    		String output = outputFormatter.format(columnValue); 
			  //  	recordString.append("'");
			    	recordString.append(output);
			    //	recordString.append("'");
		    	}
		    } else if(dataTypeString.equalsIgnoreCase("datetime") ){
		    	  Timestamp columnValue = rsColdata.getTimestamp(colMap.get(key));
		    	if(columnValue != null && !columnValue.toString().equalsIgnoreCase("null")){
				    
		    		DateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		    		String output = outputFormatter.format(columnValue); 
			    	//recordString.append("'");
			    	recordString.append(output);
			    	//recordString.append("'");
		    	}	
		    }	
		    recordString.append("~");
		    j = j + 1;
		}
		/*while(rsCol.next()){
		//	System.out.print(rsCol.getString("name"));
			System.out.print(rsColdata.getObject(rsCol.getString("name")));
			System.out.print("^");
		}*/
		//recordString.append("\n");
		//recordString.append("~");
		return recordString;
	}

	private static String getControlHeader(Map colMap, Map colMapDataType, Map colMapDataLength, int columnSize, String tableName){
		
		StringBuilder sb= new StringBuilder();
		//sb.append("options  ( skip=1 ) ");
		//sb.append(System.lineSeparator());
		sb.append("LOAD DATA");
		 sb.append(System.lineSeparator());
		 sb.append("INFILE ");
		 sb.append(" * ");
			
		 //sb.append("../data/");
		 //sb.append(tableName);
		 //sb.append(".txt");
		 sb.append(System.lineSeparator());
		 sb.append("TRUNCATE");
		 sb.append(System.lineSeparator());
		 sb.append("INTO TABLE ");
		 if(tableName.equalsIgnoreCase("SUPPLIERCOMPANYPROFILE") || tableName.equalsIgnoreCase("SUPPLIERCOMPANYPROFILE_HISTORY") || tableName.equalsIgnoreCase("SUPPLIERFACILITYPROFILE") ||
				 tableName.equalsIgnoreCase("SUPPLIERFACILITYPROFILE_HISTOR")){
		 sb.append(tableName + "_TEMP");
		 }else{
			 sb.append(tableName);
		 }
		 sb.append(System.lineSeparator());
		 sb.append("FIELDS TERMINATED BY \"^\" ");						
		 //sb.append("  optionally enclosed by '\'' TRAILING NULLCOLS");
		 sb.append(" TRAILING NULLCOLS");
		 sb.append(System.lineSeparator());
		 sb.append("(");
		 int count =0;
		 int j=1;
			
			
			for (int i=1; i< columnSize; i++) {
				count+=1;
				 String key = "col" + i;
				  //  if(rsmeta.)
				    String dataTypeString = (String)colMapDataType.get(key);
				    Object columnValue = colMap.get(key);
				    Object colLength = colMapDataLength.get(key);
				   
				if(count>1){
					sb.append(",");
				}
				
				 sb.append(columnValue);
				 sb.append(getDataType(dataTypeString, colLength));
				 sb.append(System.lineSeparator());
													
					
				}
			sb.append( ")");
			//System.out.println("contorlfile for  " + tableName);
			//System.out.println( sb.toString());
			//writeToAFile(sb.toString(), tableName);	
			
		
		return sb.toString();
	}
	
	private static String getDataType(String dataTypeString, Object colMapDataLength) {
		
		String colDataType=" ";
		String dataType = dataTypeString;
		
		 if(dataTypeString.equalsIgnoreCase("char") || dataTypeString.equalsIgnoreCase("image") ||  dataTypeString.equalsIgnoreCase("nchar") 
		    		||  dataTypeString.equalsIgnoreCase("ntext") || dataTypeString.equalsIgnoreCase("nvarchar") || dataTypeString.equalsIgnoreCase("varchar") || dataTypeString.equalsIgnoreCase("sysname") || dataTypeString.equalsIgnoreCase("uniqueidentifier") ){
			 
			 if(Integer.parseInt(colMapDataLength.toString()) > 256){
			 colDataType = colDataType + "CHAR" + "(" + colMapDataLength + ")";
			 }else{
				 colDataType = colDataType + "CHAR" + " ";
			 }
			 
		    }else if(dataTypeString.equalsIgnoreCase("bit") || dataTypeString.equalsIgnoreCase("int") || dataTypeString.equalsIgnoreCase("numeric") ){
		    	colDataType = colDataType + "DECIMAL EXTERNAL" + " ";
		    }else if(dataTypeString.equalsIgnoreCase("date") ){
		    	colDataType = colDataType + "DATE \"YYYY-MM-DD HH24:MI:SS\"" + " ";
		    } else if(dataTypeString.equalsIgnoreCase("datetime") ){
		    	colDataType = colDataType + "TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"" + " ";
		    }	
		 
		 
		/*String colDataType=" ";
		String dataType = rsCol.getString("DATA_TYPE");
		if(dataType.equalsIgnoreCase("NVARCHAR2") || dataType.equalsIgnoreCase("CHAR") ||  dataType.equalsIgnoreCase("CLOB")
				|| dataType.equalsIgnoreCase("NCHAR") || dataType.equalsIgnoreCase("NCLOB") || dataType.equalsIgnoreCase("VARCHAR2")){
			
			colDataType = colDataType + "CHAR" + "(" + rsCol.getString("data_length") + ")";
		}
		else if(dataType.equalsIgnoreCase("NUMBER") || dataType.equalsIgnoreCase("FLOAT")){
			colDataType = colDataType + "DECIMAL EXTERNAL" + " ";
		}
		else if(dataType.equalsIgnoreCase("DATE")){
			colDataType = colDataType + "DATE \"YYYY-MM-DD HH24:MI:SS\"" + " ";
		}
			
		else if(dataType.equalsIgnoreCase("TIMESTAMP(6)")){
			colDataType = colDataType + "TIMESTAMP \"YYYY-MM-DD HH24:MI:SS.FF\"" + " ";
		}*/
		
		
		return colDataType;
	}
	
	
	
	private static void createFolder(String folderDir) throws IOException {
		File fileDir = new File(folderDir);
		if(!fileDir.exists()){
			fileDir.mkdirs();
		}
		
	}
	
	 public static void delete(File file)
		    	throws IOException{
		 
		    	if(file.isDirectory()){
		 
		    		//directory is empty, then delete it
		    		if(file.list().length==0){
		 
		    		   file.delete();
		    		   System.out.println("Directory is deleted : " 
		                                                 + file.getAbsolutePath());
		 
		    		}else{
		 
		    		   //list all the directory contents
		        	   String files[] = file.list();
		 
		        	   for (String temp : files) {
		        	      //construct the file structure
		        	      File fileDelete = new File(file, temp);
		 
		        	      //recursive delete
		        	     delete(fileDelete);
		        	   }
		 
		        	   //check the directory again, if empty then delete it
		        	   if(file.list().length==0){
		           	     file.delete();
		        	     System.out.println("Directory is deleted : " 
		                                                  + file.getAbsolutePath());
		        	   }
		    		}
		 
		    	}else{
		    		//if file, then delete it
		    		file.delete();
		    		System.out.println("File is deleted : " + file.getAbsolutePath());
		    	}
		    }
	 
	 private static Properties loadPropFile(String fileName) throws FileNotFoundException{

		 Properties properties = new Properties();
			InputStream inputStream = new FileInputStream(fileName);
			try {
				properties.load(inputStream);
			} catch (IOException e) {
				System.out.println("Unable to load CEPErrors properties" + e.getMessage());
				e.printStackTrace();
			}
			return properties;
		}
	 
	 /**
	     * Size of the buffer to read/write data
	     */
	    private static final int BUFFER_SIZE = 4096;
	    /**
	     * Extracts a zip file specified by the zipFilePath to a directory specified by
	     * destDirectory (will be created if does not exists)
	     * @param zipFilePath
	     * @param destDirectory
	     * @throws IOException
	     */
	    public static void unzip(InputStream zipFilePath, String destDirectory) throws IOException {
	        File destDir = new File(destDirectory);
	        if (!destDir.exists()) {
	            destDir.mkdir();
	        }
	        ZipInputStream zipIn = new ZipInputStream(zipFilePath);
	        ZipEntry entry = zipIn.getNextEntry();
	        // iterates over entries in the zip file
	        while (entry != null) {
	            String filePath = destDirectory + File.separator + entry.getName();
	            if (!entry.isDirectory()) {
	                // if the entry is a file, extracts it
	                extractFile(zipIn, filePath);
	            } else {
	                // if the entry is a directory, make the directory
	                File dir = new File(filePath);
	                dir.mkdir();
	            }
	            zipIn.closeEntry();
	            entry = zipIn.getNextEntry();
	        }
	        zipIn.close();
	    }
	    /**
	     * Extracts a zip entry (file entry)
	     * @param zipIn
	     * @param filePath
	     * @throws IOException
	     */
	    private static void extractFile(ZipInputStream zipIn, String filePath) throws IOException {
	        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(filePath));
	        byte[] bytesIn = new byte[BUFFER_SIZE];
	        int read = 0;
	        while ((read = zipIn.read(bytesIn)) != -1) {
	            bos.write(bytesIn, 0, read);
	        }
	        bos.close();
	        System.out.println("Control file copied " +  filePath);
	    } 

	 
	 public static void copyDirectory(File sourceLocation , File targetLocation)
			    throws IOException {

		 System.out.println("copy control files from " + sourceLocation.getAbsolutePath() + " to " + targetLocation.getAbsolutePath() );	
			        if (sourceLocation.isDirectory()) {
			            if (!targetLocation.exists()) {
			                targetLocation.mkdir();
			            }

			            String[] children = sourceLocation.list();
			            for (int i=0; i<children.length; i++) {
			                copyDirectory(new File(sourceLocation, children[i]),
			                        new File(targetLocation, children[i]));
			            }
			        } else {
			        	System.out.println("copys control file " + sourceLocation.getAbsolutePath());
			            InputStream in = new FileInputStream(sourceLocation);
			            OutputStream out = new FileOutputStream(targetLocation);

			            // Copy the bits from instream to outstream
			            byte[] buf = new byte[1024];
			            int len;
			            while ((len = in.read(buf)) > 0) {
			                out.write(buf, 0, len);
			            }
			            in.close();
			            out.close();
			        }
			    }
	 
	 
	 
}
