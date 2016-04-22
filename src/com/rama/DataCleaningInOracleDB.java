package com.rama;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class DataCleaningInOracleDB {
	/**
	 * 
	 */
	public DataCleaningInOracleDB() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * http://stackoverflow.com/questions/9100722/data-not-getting-populated-from-sql-loader
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		String propertiesFile = System.getProperty("propfile");
		if(propertiesFile == null || propertiesFile.isEmpty()){
			propertiesFile = args[0];
		}
		System.out.println("File Name  " + propertiesFile );
		
		Properties props = loadPropFile(propertiesFile);
		
		if(props == null || props.isEmpty()){
			throw new Exception("Please provide propertis file as a input String");
		}
		
		

		
		String cepUser = "CEP_MIG05";
		String cepPassword = "CEP_MIG05";
		String cepConnectionString = "jdbc:oracle:thin:@//localhost:1521/orcl";
	

		
		
		
		Class.forName("oracle.jdbc.driver.OracleDriver");	
		Connection con = DriverManager.getConnection(cepConnectionString,cepUser, cepPassword);
		
		//verify data and print to a file
		//verifyDataAcrossTables(con);
		
		//updated the modified data
		
		//load the file
		
		// create update statments and update by getting column details
		UpdateData(con, props.getProperty("dataToModifyFile"), cepUser);
		

	}

	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void UpdateData(Connection con, String dataCleanFile, String cepUser) throws IOException, SQLException{
		
		
               
		
		
		
	}
	private static void verifyDataAcrossTables(Connection con) {
		try {
			//Connection con = oracleDataSource.getConnection();
			PreparedStatement stmt = con.prepareStatement("select * from user_objects where OBJECT_TYPE='TABLE' order by OBJECT_NAME");
			ResultSet rs = stmt.executeQuery();	
			
			
		
			while(rs.next()){
				String tableName =  rs.getString("object_name");
				if (!tableName.startsWith("TT")){
					
					  PreparedStatement stamtRecords = con.prepareStatement("select count(*) count from " + tableName);
					  ResultSet rsCount = stamtRecords.executeQuery();
					  while(rsCount.next()){
						  System.out.println("Count for table " + tableName + " : " + rsCount.getLong("count"));
					  }
						}
					}
				
			
		  } catch (Exception e) {
		   
		   e.printStackTrace();
		  }
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
	 

}
