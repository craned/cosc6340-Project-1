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
//	public static HashMap<String, String> tableSchemas = new HashMap<String, String>(); 
	public static ArrayList<Table> tables = new ArrayList<>();
	public static HashMap<String, Table> tableData = new HashMap<>();

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
        Connection conn;
        try {
            conn = DriverManager.getConnection(
					"jdbc:vertica://129.7.243.243:5433/cosc6340s17", myProp);
		} catch (SQLException e) {
            System.err.println("Could not connect to database.");
			e.printStackTrace();
			return;
		}

		if (conn != null) {
			System.out.println("Connection successful.");
		} else {
			System.out.println("Failed to make connection!");
			return;
		}

		extractColumns();
		extractData(conn);
    	
    	verify1NF(conn);
	}
	
	public static ResultSet executeQuery(String sqlStatement) {
		try {
			return st.executeQuery(sqlStatement);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private static void verify1NF(Connection connection)
	{
		try {
			// TODO: use HashMap to iterate through table schemas
			for (Table t : tables) {
				st = connection.createStatement();
				String query = "select count(*) from public." + t.name;
				int index = 0;
				String whereClause = "";
				for (String key : t.keys) {
					whereClause += (index == 0 ? "" : " or ") + key + " IS NULL";
					index++;
				}
				if (!whereClause.isEmpty()) {
					query += " where " + whereClause;
				}
				
	//			String query = "select * from public.testt";
				ResultSet rs = executeQuery(query);
	            while(rs.next()){
	            	if (rs.getInt(1) > 0) {
	            		System.out.println("1NF Validation Error: null candidate key");
	            		return;
	            	}
	            }
	            
	            query = "select count(*) employeeid from public." + t.name + " group by employeeid";
	            rs = executeQuery(query);
	            while(rs.next()){
	            	if (rs.getInt(1) > 1) {
	            		System.out.println("1NF Validation Error: duplicate candidate keys");
	            		return;
	            	}
	            }
			}
		} catch (SQLException e) {
			System.out.println("1NF query failed.");
			System.out.println(e.getMessage());
			e.printStackTrace();
			
			return;
		}
	}
	
	private static void extractColumns()
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
	        	Table table = new Table(tableName, columns);
	        	tables.add(table);
	        	tableData.put(tableName, table);
	        	
//	        	candidateKeysStr = candidateKeysStr.substring(0, candidateKeysStr.length()-1);
//	        	tableSchemas.put(tableName, candidateKeysStr);
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
	
	private static void extractData(Connection connection)
	{
		try {
			for (Table t : tables) {
				ArrayList<String> cols = new ArrayList<>();
				ArrayList<Integer> colData = new ArrayList<>();
				cols.addAll(t.keys);
				cols.addAll(t.nonKeys);
				// TODO: use HashMap to iterate through table schemas
				for (String col : cols) {
					st = connection.createStatement();
					String query = "select " + col + " from public." + t.name;
					
					ResultSet rs = executeQuery(query);
		            while(rs.next()){
		            	colData.add(rs.getInt(1));
		            }
					int[] colDataTransfer = new int[colData.size()];
					for (int i = 0; i < colData.size(); i++) {
						colDataTransfer[i] = colData.get(i);
					}
					
					t.colData.put(col, colDataTransfer);
		        }
			}
		} catch (SQLException e) {
			System.out.println("1NF query failed.");
			System.out.println(e.getMessage());
			e.printStackTrace();
			
			return;
		}
	}

}