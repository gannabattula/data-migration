/**
 * 
 */
package com.rama;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author rgannaba
 *
 */
public class SqlLoaderScript {

	/**
	 * 
	 */
	public SqlLoaderScript() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * http://stackoverflow.com/questions/9100722/data-not-getting-populated-from-sql-loader
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		//ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(Constants.APPLICATION_TEST_CONFIG_XML);

		//OracleDataSource oracleDataSource = ctx.getBean("oracleDataSource", OracleDataSource.class);
		
		String dataMigrationFolder ="d:\\opt\\cep\\mcd\\data_migration";
		String sqlloaderFileDir = dataMigrationFolder;
		String sqlloaderFileName = "sqlloader.bat";
		String dataFileFolder=dataMigrationFolder + "\\data";
		String logfilesDir = dataMigrationFolder + "\\logs";
		String controlFilesfolderDir = dataMigrationFolder + "\\control";
		String badFileFolderDir = dataMigrationFolder + "\\bad";
		
		String cepUser = "CEP_MIG";
		String cepPassword = "CEP_MIG";
		String cepConnectionString = "jdbc:oracle:thin:@//localhost:1521/orcl";
		String env="windows";

		String pathNameSqlloader= sqlloaderFileDir +  File.separatorChar + sqlloaderFileName;

		
		Class.forName("oracle.jdbc.driver.OracleDriver");	
		Connection con = DriverManager.getConnection(cepConnectionString,cepUser, cepPassword);
				
		try {
			//Connection con = oracleDataSource.getConnection();
			PreparedStatement stmt = con.prepareStatement("select * from user_objects where OBJECT_TYPE='TABLE' order by OBJECT_NAME");
			ResultSet rs = stmt.executeQuery();	
			
			PrintWriter pwSqlloader = getFilewriterByCreatingAFile(sqlloaderFileDir, pathNameSqlloader);
			
		
			while(rs.next()){
				String tableName =  rs.getString("object_name");
				if (!tableName.startsWith("TT")){
						 
						 StringBuffer sbSqlloader  = new StringBuffer();
						 sbSqlloader.append("sqlldr userid=");
						 sbSqlloader.append(cepUser);
						 sbSqlloader.append("/");
						 sbSqlloader.append(cepPassword);
						 sbSqlloader.append(" control=" + controlFilesfolderDir + "\\"  + tableName + ".ctl"  );
						 sbSqlloader.append(" data=" + dataFileFolder + "\\"  + tableName + ".data"  );
						 sbSqlloader.append(" log=" + logfilesDir + "\\" + tableName + ".log ");
						 sbSqlloader.append(" BAD=" + badFileFolderDir + "\\" + tableName + ".bad ");						 
						 sbSqlloader.append(" direct=y ");
						 pwSqlloader.println(sbSqlloader.toString());
						 
						 System.out.println("Contorl file is added for sqlloader file for table " + tableName);
						 
					}//tables loop
					
					}
			pwSqlloader.close();
			
			
		  } catch (Exception e) {
		   
		   e.printStackTrace();
		  }

	}

	/**
	 * @param rsCol
	 * @param tableName
	 * @param sb
	 * @throws SQLException
	 */
	private static void getCtlText(ResultSet rsCol, String tableName,
			StringBuilder sb) throws SQLException {
		sb.append("LOAD DATA");
		 sb.append(System.lineSeparator());
		 sb.append("INFILE ");
		 sb.append("../data/");
		 sb.append(tableName);
		 sb.append(".txt");
		 sb.append(System.lineSeparator());
		 sb.append("APPEND INTO TABLE ");
		 sb.append(tableName);
		 sb.append(System.lineSeparator());
		 sb.append("FIELDS TERMINATED BY \"^\" ");						
		 sb.append("  optionally enclosed by '\"' TRAILING NULLCOLS");
		 sb.append(System.lineSeparator());
		 sb.append("( ");
		 sb.append( rsCol.getString("COLUMN_NAME"));
		 sb.append(getDataType(rsCol));
		 sb.append(System.lineSeparator());
		 sb.append( ", ");
			sb.append( ")");
	}
	
	private static void writeToAFile(String fileContent, String tableName) throws IOException{
		String filePath="D:\\Atlas-galt\\MCD\\data-migration\\ctrl";
		
		File fileD = new File(filePath);
		if(!fileD.exists()){
			fileD.mkdirs();
		}
		
		String fileName = filePath + File.separator + tableName + ".ctl";
		File file = new File(fileName);
		file.createNewFile();
		FileWriter  fileWriter = new FileWriter(file.getAbsolutePath());
		fileWriter.write(fileContent);
		fileWriter.close();
	}

	private static String getDataType(ResultSet rsCol) throws SQLException{
		String colDataType=" ";
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
		}
		
		
		return colDataType;
	}
	
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
		
		  FileWriter fw = new FileWriter(filePath,true);
		  BufferedWriter bw = new BufferedWriter(fw);
		  PrintWriter pw = new PrintWriter(bw);
		return pw;
	}

	
/*	NVARCHAR2
	TIMESTAMP(6)
	FLOAT
	NUMBER
	CHAR
	CLOB
	NCHAR
	DATE
	NCLOB
	VARCHAR2
	optionally enclosed by '"' TRAILING NULLCOLS*/

}
