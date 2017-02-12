package db.systems;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class Main {

    private static Statement st = null;
	public static HashMap<String, String> candidateKeys = new HashMap<String, String>(); 

	public static void main(String[] argv) {
		
		try {
            Class.forName("com.vertica.jdbc.Driver");
		} catch (ClassNotFoundException e) {
            // Could not find the driver class. Likely an issue
            // with finding the .jar file.
            System.err.println("Could not find the JDBC driver class.");
            e.printStackTrace();
            return;
		}

        // Create property object to hold username & password
        Properties myProp = new Properties();
        myProp.put("user", "team06");
        myProp.put("password", "5ZvwXNCE");
        
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:vertica://129.7.243.243:5433/cosc6340s17", myProp);
		} catch (SQLException e) {
            System.err.println("Could not connect to database.");
			e.printStackTrace();
			return;
		}

		if (connection != null) {
			System.out.println("Connection successful.");
		} else {
			System.out.println("Failed to make connection!");
		}

    	extractCandidateKeys();

		try {
			Statement s = connection.createStatement();
			String query = "select * from T";
			ResultSet rs = s.executeQuery(query);
			
            while(rs.next()){
                System.out.println( rs.getInt(1) + " " + rs.getInt(2)+ " " + rs.getInt(3));
             }
		} catch (SQLException e) {
			System.out.println("Query failed.");
			System.out.println(e.getMessage());
			e.printStackTrace();
			
			return;
		}
	}
	
	public static ResultSet executeQuery(String sqlStatement) {
		try {
			return st.executeQuery(sqlStatement);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static void extractCandidateKeys()
	{
		BufferedReader br = null;
		try {
	    	FileInputStream fstream = new FileInputStream("db_schema");
	        br = new BufferedReader(new InputStreamReader(fstream));
	        String schema;
	        
	        while ((schema = br.readLine()) != null) {
	        	String tableName = schema.substring(0, schema.indexOf("("));
	        	String cols = schema.substring(schema.indexOf("(")+1, schema.length()-1);
	        	
	        	String[] columns = cols.split(",");
	        	System.out.println (tableName);
	        	System.out.println (cols);
	        	String candidateKeysStr = "";
	        	for (int i = 0; i < columns.length; i++) {
	        		String col = columns[i];
	        		if (col.contains("k")) {
	        			String key = col.substring(0, col.indexOf("("));
	        			System.out.println (key);
	        			candidateKeysStr += key + "|";
	        		}
	        	}
	        	
	        	candidateKeysStr.substring(0, candidateKeysStr.length()-1);
	        	candidateKeys.put(tableName, candidateKeysStr);
	        }
		} catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (IOException e) {
                
            }
        }
	}

}