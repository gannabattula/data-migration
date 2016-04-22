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
 * @author RGANNABA
 *
 */
public class OracleCtlFilesGeneration {

	/**
	 * 
	 */
	public OracleCtlFilesGeneration() {
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
		
		
		String sqlloaderFileDir = "D:\\data-migration";
		String sqlloaderFileName = "sqlloader.bat";
		String dataFileFolder="D:\data-migration\\data";
		String logfilesDir ="D:\\data-migration\\logs";
		String controlFilesfolderDir = "D:\\data-migration\\control_";

		
		String cepUser = "rama";
		String cepPassword = "rama";
		String cepConnectionString = "jdbc:oracle:thin:@//localhost:1521/orcl";
		String env="windows";

		
		
		
		Class.forName("oracle.jdbc.driver.OracleDriver");	
		Connection con = DriverManager.getConnection(cepConnectionString,cepUser, cepPassword);
		
		
		try {
			//Connection con = oracleDataSource.getConnection();
			PreparedStatement stmt = con.prepareStatement("select * from user_objects where OBJECT_TYPE='TABLE' order by OBJECT_NAME");
			ResultSet rs = stmt.executeQuery();	
			
			
		
			while(rs.next()){
				String tableName =  rs.getString("object_name");
				if (!tableName.startsWith("TT")){
					PreparedStatement stmtCol = con.prepareStatement("select * from all_tab_cols where owner='CEP_MIG' and table_name = '" + tableName + "' order by column_id");
					ResultSet rsCol = stmtCol.executeQuery();	
					StringBuilder sb= new StringBuilder();
					sb.append("options  ( skip=1 ) ");
					sb.append(System.lineSeparator());
					sb.append("LOAD DATA");
					 sb.append(System.lineSeparator());
					 sb.append("INFILE ");
					 sb.append(tableName);
					 sb.append(".data");
					 sb.append(System.lineSeparator());
					 sb.append("TRUNCATE");
					 sb.append(System.lineSeparator());
					 sb.append("INTO TABLE ");
					 sb.append(tableName);
					 sb.append(System.lineSeparator());
					 sb.append("FIELDS TERMINATED BY \"^\" ");						
					// sb.append("  optionally enclosed by '\"' TRAILING NULLCOLS");
					 sb.append(System.lineSeparator());
					 sb.append(" TRAILING NULLCOLS");
					 sb.append(System.lineSeparator());
					 sb.append("( ");
					 int count =0;
						while(rsCol.next()){
							count+=1;
							if(count>1){
								sb.append(",");
							}
							 sb.append( rsCol.getString("COLUMN_NAME"));
							 sb.append(getDataType(rsCol));
							 sb.append(System.lineSeparator());
								
													
								
							}
						sb.append( ")");
						System.out.println("contorlfile for  " + tableName);
						System.out.println( sb.toString());
						writeToAFile(sb.toString(), tableName, controlFilesfolderDir);	
						}
					}
				
			
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
	
	private static void writeToAFile(String fileContent, String tableName, String controlFileDir) throws IOException{
		String filePath=controlFileDir;
		
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
