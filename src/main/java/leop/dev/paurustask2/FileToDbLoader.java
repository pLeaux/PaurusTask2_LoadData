package leop.dev.paurustask2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties; 

/**
 * Instructions for testing: <br> 
 * (1) use mySQL server for testing, because "load data" sql script is mySQL specific; create only database schema, table is created by this application; <br>
 * (4) adjust settings in file "config.properties"(database settings and file path); read further instructions there <br>
 * (5) start application (class FileToDbLoader): table foo_random, defined in "load_data.sql" will be created and "load data" script executed <br>
 * (6) check imported data in table foo_random <br>
 * 
 * @author LeoP
 *
 */

public class FileToDbLoader {

	static Connection dbConnection; 
	static Properties configProperties;  
	
	public static void main(String[] args)  throws Exception {

		SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss:SS"); 
		String sql;
		
		System.out.println("Loading config params...");
		configProperties = new Properties(); 
	    ClassLoader loader = Thread.currentThread().getContextClassLoader();           
	    InputStream stream = loader.getResourceAsStream("config.properties");
	    configProperties.load(stream); 
	    stream.close();
	    System.out.println("Config loaded..."); 
	    
	    System.out.println("Connectin to database...");	    
		Class.forName(configProperties.getProperty("dbDriver"));
		System.out.println("JDBC driver loaded...");
		dbConnection = DriverManager.getConnection(
				configProperties.getProperty("dbConnection"), 
				configProperties.getProperty("dbUserName"), 
				configProperties.getProperty("dbPassword")
				); 
		System.out.println("Connected to database.");
		
		System.out.println("Re/creating destination table foo_random...");
		Statement sqlStatement = dbConnection.createStatement();  
		
		sql = configProperties.getProperty("dbDropTableSql"); 
		System.out.println(sql);
		sqlStatement.executeUpdate(sql); 
		
		sql = configProperties.getProperty("dbCreateTableSql");
		System.out.println(sql);
		sqlStatement.executeUpdate(sql); 
		 
		sql = configProperties.getProperty("dbLoadDataSql");
		sql = sql.replace("${srcFilePath}", "'" + configProperties.getProperty("srcFilePath") + "'");  // quick fix for reference not biing resolved
		// sql = sql.replace(":\\$:\\{srcFilePath:\\}", configProperties.getProperty("srcFilePath")); 
		System.out.println(String.format("Loading data (time: %s)...\n%s", dateFormat.format(new Date()), sql)); 
		sqlStatement.executeUpdate(sql); 
		if (! dbConnection.getAutoCommit())
			dbConnection.commit(); 
		System.out.println(String.format("Data loaded (time: %s). Check database table foo_random for results", dateFormat.format(new Date()))); 

	}

}
 
