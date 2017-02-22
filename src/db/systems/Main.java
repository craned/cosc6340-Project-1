package db.systems;




import java.sql.DriverManager;
import java.sql.ResultSet;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.sql.rowset.Joinable;

import com.vertica.dsi.dataengine.utilities.Updatable;
import com.vertica.jdbc.SMetaDataProxy;

public class Main {

    private static Statement st = null;
//	public static HashMap<String, String> tableSchemas = new HashMap<String, String>(); 
	public static ArrayList<Table> tables = new ArrayList<>();
	public static HashMap<String, Table> tableData = new HashMap<>();
	public static HashMap<String, ArrayList<ArrayList<String>>> decompose=new HashMap<String, ArrayList<ArrayList<String>>>();
	public static HashMap<String, String> dep=new HashMap<String,String>();
	public static HashMap<String, String> schemaMap =new HashMap<String,String>();
	public static ArrayList<String> sampvalue=new ArrayList<String>();
	public static final String newLine = "\n";
	public static final String tab = "\t";
	public static final String schemaPublic = "public";
	public static final String schemaPrivate = "team06schema";
	public static String schemaFilename = "db_schema";
	public static ArrayList<String> mainkey=new ArrayList<String>();
	public static ArrayList<String> mainvalue=new ArrayList<String>();
	
	public static Writer sqlWriter = null;
	public static Writer nfOutputWriter = null;
	public static Writer decompositionWriter = null;
	public static final String NF_SQL_FILENAME = "NF.sql";
	public static final String NF_OUTPUT_FILENAME = "NF.txt";
	public static final String DECOMPOSITION_FILENAME = "Decomposition.txt";

	public static void main(String[] argv) {
		if (argv.length > 0) {
			String arg0 = argv[0];
			String[] split = arg0.split("=");
			schemaFilename = split[1];
		}
		
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
		
		try {
			sqlWriter = new PrintWriter(NF_SQL_FILENAME, "UTF-8");
			nfOutputWriter = new PrintWriter(NF_OUTPUT_FILENAME, "UTF-8");
			
			decompositionWriter = new PrintWriter(DECOMPOSITION_FILENAME, "UTF-8");
			nfOutputWriter.write("#Table\t3NF\tFailed\tReason\n");
		extractColumns();
//		extractData(conn);
    	
//    	verify1NF(conn);
    	
    	for (String tableName : tableData.keySet()) {
            boolean isOneNF=true;
            boolean isTwoNF=true;
            boolean isThreeNF = true;
	        String reason = "";
            
    		Table t = tableData.get(tableName);
//    		String tablePlusSchema = "team06schema." + tableName;
	    	//procedure:
	        if((reason = oneNF(t.keysList, t.nonKeysList, tableName, conn)) != ""){
	            System.out.println("not 1NF");
	            nfOutputWriter.write(tableName + "\tN\t1NF\t" + reason + "\n");
	            isOneNF=false;
	            isTwoNF=false;
	            isThreeNF=false;
	            continue;
	        } else{
	        	System.out.println("PASSES 1NF");
	        }
	        
	        if(isOneNF){
	        	if (t.nonKeysList.size() == 0){
	        		System.out.println("PASSES 2NF");
	        		System.out.println("PASSES 3NF");
		            nfOutputWriter.write(tableName + "\tY\n");
		            continue;
	        	} else if (t.keysList.size()>1){
	                if((reason = twoNF(t.keysList,t.nonKeysList,tableName,conn))!=""){
	                    isTwoNF=false;
	                    isThreeNF=false;
	                    System.out.println("not 2NF");
	                    if (reason.lastIndexOf(",") == reason.length()-1) {
	                    	reason = reason.substring(0, reason.length()-1);
	                    } else if (reason.lastIndexOf(",") == reason.length()-2) {
	                    	reason = reason.substring(0, reason.length()-2);
	                    }
	    	            nfOutputWriter.write(tableName + "\tN\t2NF\t" + reason + "\n");
	    	            decompositionWriter.write("#" + tableName + " decomposition:\n");
	                    decompose2NF(tableName, twoNF(t.keysList,t.nonKeysList,tableName,conn),conn);
	                    join2nf(tableName,conn);
	        	        for (Map.Entry<String, ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
	        	        	String value = "";
	        	        	for (ArrayList<String> al : entry.getValue()) {
	        	        		for (String str : al) {
	        	        			value += str + ",";
	        	        		}
	        	        	}
        	        		value = value.substring(0, value.lastIndexOf(","));
    	    	            String result = entry.getKey() + "(" + value + ")";
    	    	            
    	    	            decompositionWriter.write(result + "\n");
	        	        	System.out.println (result);
	        	        }
	        	        
	        	        decompose.clear();
	        	        
	    	            decompositionWriter.write("#Verification:\n\n");
	        	        continue;
	                } else {
	                	System.out.println("PASSES 2NF");
	                }
	            } else {
                	System.out.println("PASSES 2NF");
                }
	        }
	        
	        if((t.nonKeysList.size()>1) && isTwoNF && (reason = threeNF(t.nonKeysList, tableName, conn))!=""){
	            isThreeNF=false;
	            System.out.println("not 3NF");
                if (reason.lastIndexOf(",") == reason.length()-1) {
                	reason = reason.substring(0, reason.length()-1);
                } else if (reason.lastIndexOf(",") == reason.length()-2) {
                	reason = reason.substring(0, reason.length()-2);
                }
	            nfOutputWriter.write(tableName + "\tN\t3NF\t" + reason + "\n");
	            decompose3NF(t.keysList, t.nonKeysList, tableName, threeNF(t.nonKeysList, tableName, conn),conn);
    	        
    	        for (Map.Entry<String, ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
    	        	String value = "";
    	        	for (ArrayList<String> al : entry.getValue()) {
    	        		for (String str : al) {
    	        			value += str + ",";
    	        		}
    	        	}
	        		value = value.substring(0, value.lastIndexOf(","));
    	            String result = entry.getKey() + "(" + value + ")";
    	            
    	            decompositionWriter.write(result + "\n");
    	        	System.out.println (result);
    	        }

		        //join(tableName, conn);
	            join1(tableName, conn);
    	        
    	        decompose.clear();
    	        
	        } else {
	        	System.out.println("PASSES 3NF");
	            nfOutputWriter.write(tableName + "\tY\n");
	        }
    	}
    	
    	sqlWriter.close();
    	nfOutputWriter.close();
    	decompositionWriter.close();
    	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void writeSQL(String sqlQuery)
	{
		try {
			sqlWriter.write(sqlQuery + newLine + newLine);
//			System.out.println(sqlQuery);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void decompose2NF(String tableName, String output,Connection conn){ // k1,k2->A
    	try{
    	Statement stmt = conn.createStatement();
    
        System.out.println("output: "+output);
        System.out.println("decompose2NF");	
        for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			System.out.println("decompose");
			String s11="create table";
			String s21="";
			ArrayList<String> key1=new ArrayList<String>();
			ArrayList<String> nonkey1=new ArrayList<String>();
			key1.addAll(entry.getValue().get(0));
			nonkey1.addAll(entry.getValue().get(1));
			for(int i=0;i<entry.getValue().get(0).size();i++){
			s21=s21+key1.get(i).toString()+" varchar(10) ,";
			}
			for(int i=0;i<nonkey1.size();i++){
				s21=s21+nonkey1.get(i).toString()+" varchar(10) ,";
				}
			s21=s21+"primary key(";
			for(int i=0;i<key1.size();i++){
				if(i<key1.size()-1)
					s21=s21+key1.get(i).toString()+",";
				else
					s21=s21+key1.get(i).toString()+"))";
				}
			String query = s11+" "+schemaMap.get(entry.getKey())+"."+entry.getKey()+"("+s21;
			//schemaMap.put(entry.getKey(), schemaPrivate); // TODO: why commented out?
			System.out.println(query);
			writeSQL(query);
			stmt.executeUpdate(query);
			
		}
		
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			String sd1="INSERT INTO ";
			String sd2="";
			ArrayList<String> key1=new ArrayList<String>();
			ArrayList<String> nonkey1=new ArrayList<String>();
			key1.addAll(entry.getValue().get(0));
			nonkey1.addAll(entry.getValue().get(1));
			for(int i=0;i<entry.getValue().get(0).size();i++){
				
				if(nonkey1.size()==0&&i==entry.getValue().get(0).size()-1)
					sd2=sd2+key1.get(i).toString()+")";
				else
					sd2=sd2+key1.get(i).toString()+",";

			}
			for(int i=0;i<nonkey1.size();i++){
				if(i<nonkey1.size()-1)
				sd2=sd2+nonkey1.get(i).toString()+",";
				else
					sd2=sd2+nonkey1.get(i).toString()+")";
					
				}
			String sd3="";
			for(int i=0;i<entry.getValue().get(0).size();i++){
				if(nonkey1.size()==0&&i==entry.getValue().get(0).size()-1)
						sd3=sd3+key1.get(i).toString();
					else
						sd3=sd3+key1.get(i).toString()+",";
					//sd3=sd3+tableName+"."+key1.get(i).toString()+",";

			}
			
			for(int i=0;i<nonkey1.size();i++){
				if(i<nonkey1.size()-1)
				sd3=sd3+tableName+"."+nonkey1.get(i).toString()+",";
				else
					sd3=sd3+tableName+"."+nonkey1.get(i).toString();
			}
			String query = sd1+" "+schemaPrivate+"."+entry.getKey()+"("+sd2+" "+" SELECT "+sd3+" FROM"+" "+schemaPrivate+"."+tableName;
			System.out.println(query);
			writeSQL(query);
			stmt.executeUpdate(query);
		}//*/
    	}
    	catch(Exception e){
    		System.out.println(e);
    	}
        //suppose you have a output string like: "T: K1->A, K2->B"
     }
    
	public static void decompose3NF(ArrayList<String> keysList,ArrayList<String> nonKeysList,String tableName, String output,Connection conn){
        //Kanya
   	 try{
   		 
   		 Statement stmt= conn.createStatement();
        System.out.println("output: "+output);
        System.out.println("decompose3NF");
    
		int count=2;
		for(Map.Entry<String,String> entry:dep.entrySet()){
			ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
			ArrayList<String> temKey=new ArrayList<String>();
			ArrayList<String> temValue=new ArrayList<String>();
			temKey.add(entry.getKey());
			temValue.add(entry.getValue());
			tem.add(temKey);
			tem.add(temValue);
			decompose.put(tableName+Integer.toString(count),tem);
			schemaMap.put(tableName+Integer.toString(count),schemaPrivate);
			mainkey.add(entry.getKey());
			System.out.println(mainkey);
			mainvalue.add(entry.getValue());
			System.out.println(mainvalue);
			count++;
			
		}
		mainkey.removeAll(mainvalue);
		System.out.println(mainkey);
		ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
		ArrayList<String> temValue=new ArrayList<String>();
		temValue.addAll(nonKeysList);
		temValue.retainAll(mainkey);
		System.out.println(temValue);
		tem.add(keysList);
		tem.add(temValue);
		decompose.put(tableName+"1", tem);
		String sc1="CREATE table";
		String s2="";
		int stc=0;
		
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			String s11="CREATE table";
			String s21="";
			ArrayList<String> key1=new ArrayList<String>();
			ArrayList<String> nonkey1=new ArrayList<String>();
			key1.addAll(entry.getValue().get(0));
			nonkey1.addAll(entry.getValue().get(1));
			for(int i=0;i<entry.getValue().get(0).size();i++){
			s21=s21+key1.get(i).toString()+" varchar(10),";
			}
			for(int i=0;i<nonkey1.size();i++){
				s21=s21+nonkey1.get(i).toString()+" varchar(10),";
				}
			s21=s21+"primary key(";
			for(int i=0;i<key1.size();i++){
				if(i<key1.size()-1)
					s21=s21+key1.get(i).toString()+",";
				else
					s21=s21+key1.get(i).toString()+"))";
				}
			String query = s11+" "+schemaMap.get(entry.getKey())+"."+entry.getKey()+"("+s21;
			//schemaMap.put(entry.getKey(), schemaPrivate); // TODO: why commented out?
			System.out.println(query);
			writeSQL(query);
			//Statement stmt= conn.createStatement();
			
			stmt.executeUpdate(query);
			System.out.println("furuuruurdurur");
			
		}
		
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			String sd1="INSERT INTO ";
			String sd2="";
			ArrayList<String> key1=new ArrayList<String>();
			ArrayList<String> nonkey1=new ArrayList<String>();
			key1.addAll(entry.getValue().get(0));
			nonkey1.addAll(entry.getValue().get(1));
			for(int i=0;i<entry.getValue().get(0).size();i++){
				if(nonkey1.size()==0&&i==entry.getValue().get(0).size()-1)
					sd2=sd2+key1.get(i).toString()+")";
				else
					sd2=sd2+key1.get(i).toString()+",";

			}
			for(int i=0;i<nonkey1.size();i++){
				if(i<nonkey1.size()-1)
				sd2=sd2+nonkey1.get(i).toString()+",";
				else
					sd2=sd2+nonkey1.get(i).toString()+")";
					
				}
			String sd3="";
			for(int i=0;i<entry.getValue().get(0).size();i++){
					sd3=sd3+tableName+"."+key1.get(i).toString()+",";

			}
			
			for(int i=0;i<nonkey1.size();i++){
				if(i<nonkey1.size()-1)
				sd3=sd3+tableName+"."+nonkey1.get(i).toString()+",";
				else
					sd3=sd3+tableName+"."+nonkey1.get(i).toString();
			}
			String query = sd1+" "+schemaMap.get(entry.getKey())+"."+entry.getKey()+"("+sd2+" "+" SELECT DISTINCT "+sd3+" FROM "+" "+schemaMap.get(tableName)+"."+tableName;
			System.out.println(query);
			writeSQL(query);
			//Statement stmt = conn.createStatement();
			stmt.executeUpdate(query);
			//System.out.println("asdf");
		}
    }
   	 catch(Exception e){
   		 System.out.println(e);
   	 }
    }
	
	public static void join1(String tableName,Connection conn){
   	 try{
   	 HashMap<String, ArrayList<ArrayList<String>>> join=new HashMap<String, ArrayList<ArrayList<String>>>();
   	 ArrayList<String> pkey=new ArrayList<String>();
 		ArrayList<String> nkey=new ArrayList<String>();
 		ArrayList<ArrayList<String>> values=new ArrayList<ArrayList<String>>();
 		if(decompose.size()>0){
 			ResultSet rs;
 			System.out.println("decompose size"+decompose.size());
 			pkey.addAll(decompose.get(tableName+"1").get(0));
			nkey.addAll(decompose.get(tableName+"1").get(1));  
			values.add(pkey);
			values.add(nkey);
			join.put(tableName+"D1",values);
			
			System.out.println(join.get(tableName+"D1")+"1 st table");
			int c=1;
			int count=2,f=0;
			for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
				//System.out.println(entry.getKey());
				ArrayList<String> pkey1=new ArrayList<String>();
	 			ArrayList<String> nkey1=new ArrayList<String>();
	 			ArrayList<String> nkeyp=new ArrayList<String>();
	 			ArrayList<ArrayList<String>> values1=new ArrayList<ArrayList<String>>();
	 			ArrayList<String> tempkeys=new ArrayList<String>();
				if(c==1){
					c=0;
					continue;
				}
				else{
					
					System.out.println(entry.getKey());
					pkey1.addAll(entry.getValue().get(0));
					nkey1.addAll(entry.getValue().get(1));
					nkeyp.addAll(nkey);
					if(!nkey.containsAll(nkey1)){
					nkey.addAll(nkey1);
					f=1;
					
					
						
					values1.add(pkey);
					values1.add(nkey);
					join.put(tableName+"D"+Integer.toString(count),values1);
					System.out.println(join.get(tableName+"D"+Integer.toString(count))+"2 st table");
					String s1="";
					for(String x:pkey){
						s1=s1+schemaMap.get(tableName)+"."+tableName+Integer.toString(count-1)+"."+x+",";
					}
					for(int i=0;i<nkeyp.size();i++){
						if(nkey1.size()==0&&i==nkeyp.size()-1)
							s1=s1+schemaMap.get(tableName)+"."+tableName+Integer.toString(count-1)+"."+nkeyp.get(i);
						else
						s1=s1+schemaMap.get(tableName)+"."+tableName+Integer.toString(count-1)+"."+nkeyp.get(i)+",";
					
					}
					
					for(int i=0;i<nkey1.size();i++){
						if(i==nkey1.size()-1)
							s1=s1+schemaMap.get(tableName)+"."+tableName+Integer.toString(count)+"."+nkey1.get(i);
						else
						s1=s1+schemaMap.get(tableName)+"."+tableName+Integer.toString(count)+"."+nkey1.get(i)+",";
					
					}
					String s2="";
					for(int i=0;i<pkey1.size();i++){
						if(i<pkey1.size()-1)
	 						s2=s2+tableName+Integer.toString(count-1)+"."+pkey1.get(i)+"="+entry.getKey()+"."+pkey1.get(i)+"and";
	 						else
	 							s2=s2+tableName+Integer.toString(count-1)+"."+pkey1.get(i)+"="+entry.getKey()+"."+pkey1.get(i);
					}
					String s3="";
					for(int i=0;i<pkey.size();i++){
						if(i<pkey.size()-1)
	 						s3=s3+tableName+Integer.toString(count-1)+"."+pkey.get(i)+" and ";
	 						else
	 							s3=s3+tableName+Integer.toString(count-1)+"."+pkey.get(i);
				}
					String query = "select "+" "+s1+" FROM "+schemaMap.get(tableName)+"."+tableName+Integer.toString(count-1)+" \nINNER JOIN "+schemaMap.get(entry.getKey())+"."+entry.getKey()+" ON "+s2;
			
			
				System.out.println(f);
			writeSQL(query);
			System.out.println(query);
				 rs = executeQuery(query);
			
					}
				}
				count++;
				f=0;
 		}
    
 		}
   	 }
   	 catch(Exception e){
   		 System.out.println(e);
   	 }
    }
	
	public static void join2nf(String tableName,Connection conn){
   	 try{
   		 ResultSet rs;
   	 HashMap<String, ArrayList<ArrayList<String>>> join=new HashMap<String, ArrayList<ArrayList<String>>>();
   	 ArrayList<String> pkey=new ArrayList<String>();
 		ArrayList<String> nkey=new ArrayList<String>();
 		ArrayList<ArrayList<String>> values=new ArrayList<ArrayList<String>>();
 		if(decompose.size()>0){
 			System.out.println("decompose size"+decompose.size());
 			pkey.addAll(decompose.get(tableName+"1").get(0));
			nkey.addAll(decompose.get(tableName+"1").get(1));  
			values.add(pkey);
			values.add(nkey);
			join.put(tableName+"D1",values);
			int rss1=0,rss2=-1;
			System.out.println(join.get(tableName+"D1")+"1 st table");
			int c=1;
			int count=2,f=0;
			for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
				//System.out.println(entry.getKey());
				ArrayList<String> pkey1=new ArrayList<String>();
	 			ArrayList<String> nkey1=new ArrayList<String>();
	 			ArrayList<String> nkeyp=new ArrayList<String>();
	 			ArrayList<ArrayList<String>> values1=new ArrayList<ArrayList<String>>();
	 			ArrayList<String> tempkeys=new ArrayList<String>();
				if(c==1){
					c=0;
					continue;
				}
				else{
					
					System.out.println(entry.getKey());
					pkey1.addAll(entry.getValue().get(0));
					nkey1.addAll(entry.getValue().get(1));
					nkeyp.addAll(nkey);
					if(!nkey.containsAll(nkey1)){
					nkey.addAll(nkey1);
					f=1;
					
					
						
					values1.add(pkey);
					values1.add(nkey);
					join.put(tableName+"D"+Integer.toString(count),values1);
					System.out.println(join.get(tableName+"D"+Integer.toString(count))+"2 st table");
					String s1="";
					for(String x:pkey){
						s1=s1+schemaMap.get(tableName+Integer.toString(count-1))+"."+tableName+Integer.toString(count-1)+"."+x+",";
					}
					for(int i=0;i<nkeyp.size();i++){
						if(nkey1.size()==0&&i==nkeyp.size()-1)
							s1=s1+schemaMap.get(tableName+Integer.toString(count-1))+"."+tableName+Integer.toString(count-1)+"."+nkeyp.get(i);
						else
						s1=s1+schemaMap.get(tableName+Integer.toString(count-1))+"."+tableName+Integer.toString(count-1)+"."+nkeyp.get(i)+",";
					
					}
					
					for(int i=0;i<nkey1.size();i++){
						if(i==nkey1.size()-1)
							s1=s1+schemaMap.get(tableName+Integer.toString(count))+"."+tableName+Integer.toString(count)+"."+nkey1.get(i);
						else
						s1=s1+schemaMap.get(tableName+Integer.toString(count))+"."+tableName+Integer.toString(count)+"."+nkey1.get(i)+",";
					
					}
					String s2="";
					for(int i=0;i<pkey1.size();i++){
						if(i<pkey1.size()-1)
	 						s2=s2+tableName+Integer.toString(count-1)+"."+pkey1.get(i)+"="+entry.getKey()+"."+pkey1.get(i)+" and ";
	 						else
	 							s2=s2+tableName+Integer.toString(count-1)+"."+pkey1.get(i)+"="+entry.getKey()+"."+pkey1.get(i);
					}
					String s3="";
					for(int i=0;i<pkey.size();i++){
						if(i<pkey.size()-1)
	 						s3=s3+tableName+Integer.toString(count-1)+"."+pkey.get(i)+" and ";
	 						else
	 							s3=s3+tableName+Integer.toString(count-1)+"."+pkey.get(i);
				}
					String query = "select"+" "+s1+" FROM "+schemaMap.get(tableName)+"."+tableName+Integer.toString(count-1)+" \nINNER JOIN "+schemaMap.get(entry.getKey())+"."+entry.getKey()+" ON "+s2;
			
			
				System.out.println(f);
			writeSQL(query);
			System.out.println(query);
				 rs = executeQuery(query);
				 while(rs.next()){
					 rss1=rs.getInt(1);
				 }
					}
				}
				count++;
				f=0;
 		}
			/*String query1 = "SELECT COUNT(*) FROM " + schemaMap.get(tableName) + "." + tableName;
			writeSQL(query1);
			ResultSet rs1 = executeQuery(query1);
			while(rs1.next()){
				rss2=rs1.getInt(1);
			}
			if(rss1==rss2){
				System.out.println("join successful");
			}*/
			
 		}
 		
   	 }
   	 catch(Exception e){
   		 System.out.println(e);
   	 }
    }
     
     public static String oneNF(ArrayList<String> keysList,ArrayList<String> nonKeysList,String tableName,Connection conn) {
         //Devin
         //use isnull in sql to check null keys
         //for finding duplicate: (select k, count(*) from R group by k)==n
    	 
    	 try {
// 			for (Table t : tables) {
 				st = conn.createStatement();
 				String query = "SELECT COUNT(*) FROM " + schemaMap.get(tableName) + "." + tableName;
 				int index = 0;
 				String whereClause = "";
 				for (String key : keysList) {
 					whereClause += (index == 0 ? "" : " or ") + key + " IS NULL";
 					index++;
 				}
// 				if (!whereClause.isEmpty()) {
// 					query += " WHERE " + whereClause;
// 				}
 				
 	//			String query = "select * from team06schema.testt";
 				writeSQL(query + " \n\tWHERE " + whereClause);
 				ResultSet rs = executeQuery(query + " WHERE " + whereClause);
 	            while(rs.next()){
 	            	if (rs.getInt(1) > 0) {
 	            		System.out.println("1NF Validation error " + schemaMap.get(tableName) + "." + tableName + ": null candidate key");
 	            		return schemaMap.get(tableName) + "." + tableName + ": null candidate key";
 	            	}
 	            }
 	            
 	            String keys = "";
 	            for (int i = 0; i < keysList.size(); i++) {
 	            	keys += keysList.get(i) + (i < keysList.size()-1 ? ", " : "");
 	            }
 	            String groupBy = " GROUP BY " + keys;
 	            String select = "SELECT COUNT(*) FROM " + schemaMap.get(tableName) + "." + tableName;
 	            query = select + groupBy;
 				writeSQL(select + newLine + tab + groupBy);
 	            rs = executeQuery(query);
 	            while(rs.next()){
 	            	if (rs.getInt(1) > 1) {
 	            		System.out.println("1NF Validation error " + schemaMap.get(tableName) + "." + tableName + ": duplicate candidate keys");
 	            		return schemaMap.get(tableName) + "." + tableName + ": duplicate candidate keys";
 	            	}
 	            }
 	            
 	            String columns = "";
 	            for (int i = 0; i < keysList.size(); i++) {
 	            	columns += keysList.get(i) + ",";
 	            }
 	            for (int i = 0; i < nonKeysList.size(); i++) {
 	            	columns += nonKeysList.get(i) + ",";
 	            }
 	            columns = columns.substring(0, columns.length()-1);
// 	            groupBy = " GROUP BY " + columns;
 	            select = "SELECT " + columns + " FROM " + schemaMap.get(tableName) + "." + tableName;
 	            query = select;
 				writeSQL(select);
 	            rs = executeQuery(query);
// 			}
 		} catch (SQLException e) {
 			System.out.println("1NF query failed.");
 			String errorMessage = e.getMessage();
 			System.out.println(errorMessage);
// 			e.printStackTrace();

 			if (errorMessage.contains("ERROR:")) {
 				return errorMessage.substring(errorMessage.indexOf(":")+2);
 			} else {
 				return errorMessage;
 			}
 		}
    	 
        return "";
     }
     public static String twoNF(ArrayList<String> keysList,ArrayList<String> nonKeysList,String tableName,Connection conn) {
         
         String output="";
         int count=2;
         try{
         if (keysList.size()>=2){
         
             
             Statement stmt = conn.createStatement();
             Statement stmt2 = conn.createStatement();
 
             for (int i = 0; i < keysList.size(); i++) {
                 for (int j = 0; j < nonKeysList.size(); j++) {
                     int value1=-1;
                     int value2=-1;
                     ResultSet rs = null;
                     String select = "SELECT SUM(ca) AS countKeyToNonKey FROM ";
                     String nestedSelect = "(SELECT distinct " + keysList.get(i)+ ", COUNT(DISTINCT "+nonKeysList.get(j)+ ") as ca FROM "+
                    		 schemaMap.get(tableName) + "." + tableName;
                     String groupBy = " GROUP BY "+keysList.get(i)+ ") AS derivedTable ;";
                     String query = select + nestedSelect + groupBy;
                     writeSQL(select + newLine + tab + nestedSelect + newLine + tab + tab + groupBy);
                     rs = stmt.executeQuery(query);
                     while(rs.next()){
                         //System.out.println(" key: "+keysList.get(i)+" nonkey: "+nonKeysList.get(j)+" count: "+rs.getInt("countKeyToNonKey") );
                         value1=rs.getInt("countKeyToNonKey");
                     }
                     ResultSet rs2= null;
                     query = "SELECT COUNT(DISTINCT " + keysList.get(i)+ ") as ck FROM "+ schemaMap.get(tableName) + "." + tableName;
                     writeSQL(query);
                     rs2 = stmt2.executeQuery(query);
                     while(rs2.next()){
                         //System.out.println("num of distinct k: "+rs2.getInt("ck") );
                         value2=rs2.getInt("ck");
                     }
                     if(value1==value2){
                         //System.out.println(keysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");                                    
                         output=output+keysList.get(i)+"->"+ nonKeysList.get(j)+", ";    
                         ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
                         ArrayList<String> temKey=new ArrayList<String>();
                         ArrayList<String> temValue=new ArrayList<String>();
                       
                        	 temKey.add(keysList.get(i));
		                     temValue.add(nonKeysList.get(j));
		                     tem.add(temKey);
		                     tem.add(temValue);
		                     decompose.put(tableName+Integer.toString(count),tem);
		                    
		                     sampvalue.add(nonKeysList.get(j));
		                     schemaMap.put(tableName+Integer.toString(count), schemaPrivate);
		                     System.out.println(tableName+Integer.toString(count)+schemaMap.get(tableName+Integer.toString(count)));
		                     count++;
                     }
                 }
             }
             Statement stmt3 = conn.createStatement();
             Statement stmt4 = conn.createStatement();
             if(keysList.size()>=3){
                 for (int i = 0; i < keysList.size()-1; i++) {
                     for (int j = i+1; j < keysList.size(); j++) {
                         if(i==j) continue;                             
                         for (int k = 0; k < nonKeysList.size(); k++) {
                             
                             
                             int value1=-1;
                             int value2=-1;
                             
                             ResultSet rs = null;
                             String select = "SELECT sum(ca) AS countKeyToNonKey FROM ";
                             String innerSelect = "(SELECT DISTINCT " + keysList.get(i)+" , "+keysList.get(j)+ ", COUNT(DISTINCT "+
          		 					nonKeysList.get(k) + ") AS ca FROM "+ schemaMap.get(tableName) + "." + tableName;
                             String groupBy = " GROUP BY "+keysList.get(i)+" , "+keysList.get(j)+ ") AS derivedTable ;";
                             String query = select + innerSelect + groupBy;
                             writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                             rs = stmt3.executeQuery(query);
                             
                             while(rs.next()){
                             //System.out.println(" key: "+keysList.get(i)+" nonkey: "+nonKeysList.get(j)+" count: "+rs.getInt("countKeyToNonKey") );
                             value1=rs.getInt("countKeyToNonKey");
                             }
                             ResultSet rs2= null;
                             select = "SELECT COUNT(*) AS ck FROM ";
                             innerSelect = "(SELECT "+keysList.get(i)+","+keysList.get(j)+" FROM "+ schemaMap.get(tableName) + "." + tableName;
                             groupBy = " GROUP BY "+ keysList.get(i)+","+keysList.get(j)+") AS derived";
                             query = select + innerSelect + groupBy;
                             writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                             rs2 = stmt4.executeQuery(query);
                             while(rs2.next()){
                                 value2=rs2.getInt("ck");
                             }
                             if(value1==value2){
                                 //System.out.println(keysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");                                    
                                 output=output+(keysList.get(i)+","+keysList.get(j)+"->"+ nonKeysList.get(k)+", ");    
                            
                                 ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
                     			ArrayList<String> temKey=new ArrayList<String>();
                     			ArrayList<String> temValue=new ArrayList<String>();
                               
		                             temKey.add(keysList.get(i));
		                             temKey.add(keysList.get(j));
		                             temValue.add(nonKeysList.get(k));
		                             tem.add(temKey);
		                             tem.add(temValue);
		                             decompose.put(tableName+Integer.toString(count),tem);
		                             
		                             sampvalue.add(nonKeysList.get(k));
		                             schemaMap.put(tableName+Integer.toString(count), schemaPrivate);
		                             System.out.println(tableName+Integer.toString(count)+schemaMap.get(tableName+Integer.toString(count)));
                                count++;
                             }
                             
                             
                         }
                     }
                 }
             
             Statement stmt5 = conn.createStatement();
             Statement stmt6 = conn.createStatement();
             if (keysList.size()==4){
                 for (int i = 0; i < keysList.size()-2; i++) {
                     for (int j = i+1; j < keysList.size()-1; j++) {
                         for(int l=j+1; l<keysList.size();l++){
                             for (int k = 0; k < nonKeysList.size(); k++) {
                                 int value1=-1;
                                 int value2=-1;
                                 
                                 ResultSet rs = null;
                                 String select = "SELECT SUM(ca) AS countKeyToNonKey FROM ";
                                 String innerSelect = "(SELECT distinct " + keysList.get(i)+" , "+keysList.get(j)+" , "+keysList.get(l)+
                              		 	", COUNT(DISTINCT "+ nonKeysList.get(k) + ") AS ca FROM "+ schemaMap.get(tableName) + "." + tableName;
                                 String groupBy = " GROUP BY "+keysList.get(i)+" , "+keysList.get(j)+" , "+keysList.get(l)+ ") AS derivedTable ;";
                                 
                                 String query = select + innerSelect + groupBy;
                                 writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                                 rs = stmt5.executeQuery(query);
                                 while(rs.next()){        
                                     value1=rs.getInt("countKeyToNonKey");
                                 }
                                 ResultSet rs2= null;
                                 select = "SELECT COUNT(*) AS ck FROM ";
                                 innerSelect = "(SELECT "+keysList.get(i)+","+keysList.get(j)+" , "+keysList.get(l)+" FROM "+ schemaMap.get(tableName) + "." +
                              		 	tableName;
                                 groupBy = " GROUP BY "+ keysList.get(i)+","+keysList.get(j)+" , "+keysList.get(l)+") AS derived";
                                 
                                 query = select + innerSelect + groupBy;
                                 writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                                 rs2 = stmt6.executeQuery(query);
                                 while(rs2.next()){
                                     value2=rs2.getInt("ck");
                                 }
                                 if(value1==value2){
                                     //System.out.println(keysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");                                    
                                     output=output+(keysList.get(i)+","+keysList.get(j)+","+keysList.get(l)+"->"+ nonKeysList.get(k)+", ");    
                                     ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
                         			ArrayList<String> temKey=new ArrayList<String>();
                         			ArrayList<String> temValue=new ArrayList<String>();

		                                 temKey.add(keysList.get(i));
		                                 temKey.add(keysList.get(j));
		                                 temKey.add(keysList.get(l));
		                                 temValue.add(nonKeysList.get(k));
		                                 tem.add(temKey);
		                                 tem.add(temValue);
		                                 decompose.put(tableName+Integer.toString(count),tem);
		                                 sampvalue.add(nonKeysList.get(k));
		                                 schemaMap.put(tableName+Integer.toString(count), schemaPrivate);
		                                 count++;
                                 }
                                 }
                             }
                         }
                     }
                 }
             
             }                 
         }    
         
         ArrayList<String> sampkey1=new ArrayList<String>();
         sampkey1.addAll(nonKeysList);
         sampkey1.removeAll(sampvalue);
         ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
		ArrayList<String> temKey=new ArrayList<String>();
		temKey.addAll(keysList);
     
     
	     tem.add(temKey);
	     tem.add(sampkey1);
	     decompose.put(tableName+Integer.toString(1),tem);
	     schemaMap.put(tableName+Integer.toString(1), schemaPrivate);
         }catch(SQLException e) {
             System.err.println("Could not check the 2NF");
         e.printStackTrace();
         }
         //System.out.println("output= "+ output);
         return output;
     }
     
     
     public static String threeNF(ArrayList<String> nonKeysList,String tableName,Connection conn) {
         
         String output="";
         try{
         if(nonKeysList.size()>=2){
         
             Statement stmt = conn.createStatement();
             Statement stmt2 = conn.createStatement();
             
             for (int i = 0; i < nonKeysList.size(); i++) {
                 for (int j = 0; j < nonKeysList.size(); j++) {
                     if (i==j) continue;
                     
                         int value1=-1;
                         int value2=-1;
                         ResultSet rs = null;
                         String select = "SELECT SUM(ca) AS countnonKeyToNonKey FROM ";
                         String innerSelect = "(SELECT DISTINCT " + nonKeysList.get(i)+ ", COUNT(DISTINCT "+nonKeysList.get(j)+ ") as ca FROM "+
                        		 schemaMap.get(tableName) + "." +tableName;
                         String groupBy = " GROUP BY "+nonKeysList.get(i)+ ") AS derivedTable ;";
                         
                         String query = select + innerSelect + groupBy;
                         writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                         rs = stmt.executeQuery(query);
                         while(rs.next()){
                             //System.out.println(" key: "+nonKeysList.get(i)+" nonkey: "+nonKeysList.get(j)+" count: "+rs.getInt("countnonKeyToNonKey") );
                             value1=rs.getInt("countnonKeyToNonKey");
                         }
                         ResultSet rs2= null;
                         query = "SELECT COUNT(DISTINCT " + nonKeysList.get(i)+ ") AS ck FROM "+ schemaMap.get(tableName) + "." + tableName;
                         writeSQL(query);
                         rs2 = stmt2.executeQuery(query);
                         while(rs2.next()){
                             //System.out.println("num of distinct k: "+rs2.getInt("ck") );
                             value2=rs2.getInt("ck");
                         }
                         if(value1==value2){
                                 //System.out.println(nonKeysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");
                                 output=output+nonKeysList.get(i)+"->"+ nonKeysList.get(j)+", ";                                 
                         if(!dep.containsKey(nonKeysList.get(i)))
                                 dep.put(nonKeysList.get(i), nonKeysList.get(j));
                         }
                 }
             }
             
             if(nonKeysList.size()>=3){
             Statement stmt3 = conn.createStatement();
             Statement stmt4 = conn.createStatement();
                 for (int i = 0; i < nonKeysList.size()-1; i++) {
                     for (int j = i+1; j < nonKeysList.size(); j++) {
                         if (i==j) continue;
                         for (int k = 0; k < nonKeysList.size(); k++){
                             if((i==k) || (j==k)) continue;
                             int value1=-1;
                             int value2=-1;
                             
                             ResultSet rs = null;
                             String select = "SELECT SUM(ca) AS countKeyToNonKey FROM ";
                             String innerSelect = "(SELECT distinct " + nonKeysList.get(i)+" , "+nonKeysList.get(j)+
              		 				", COUNT(DISTINCT "+ nonKeysList.get(k) + ") AS ca FROM "+ schemaMap.get(tableName) + "." + tableName;
                             String groupBy = " GROUP BY "+nonKeysList.get(i)+" , "+ nonKeysList.get(j)+ ") AS derivedTable ;";
                             
                             String query = select + innerSelect + groupBy;
                             writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                             rs = stmt3.executeQuery(query);
                             
                             while(rs.next()){
                             //System.out.println(" key: "+keysList.get(i)+" nonkey: "+nonKeysList.get(j)+" count: "+rs.getInt("countKeyToNonKey") );
                             value1=rs.getInt("countKeyToNonKey");
                             }
                             ResultSet rs2= null;
                             select = "SELECT COUNT(*) AS ck FROM ";
                             innerSelect = "(SELECT "+nonKeysList.get(i)+","+nonKeysList.get(j)+" FROM "+ schemaMap.get(tableName) + "." + tableName;
                             groupBy = " GROUP BY "+ nonKeysList.get(i)+","+nonKeysList.get(j)+") AS derived";
                             
                             query = select + innerSelect + groupBy;
                             writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                             rs2 = stmt4.executeQuery(query);
                             
                             while(rs2.next()){
                                 value2=rs2.getInt("ck");
                             }
                             if(value1==value2){
                                 //System.out.println(keysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");                                    
                                 output=output+(nonKeysList.get(i)+","+nonKeysList.get(j)+"->"+ nonKeysList.get(k)+", ");    
                             }
                             
                         }
                     }
                 }
                if(nonKeysList.size()>=4){
                   Statement stmt5 = conn.createStatement();
                   Statement stmt6 = conn.createStatement();
                   for (int i = 0; i < nonKeysList.size()-2; i++) {
                	   for (int j = i+1; j < nonKeysList.size()-1; j++) {
                         for(int l=j+1; l<nonKeysList.size();l++) {
                             for (int k = 0; k < nonKeysList.size(); k++) {
                                 if((i==k)||(j==k)||(l==k)) continue;
                                 int value1=-1;
                                 int value2=-1;
                                 
                                 ResultSet rs = null;
                                 String select = "SELECT SUM(ca) AS countKeyToNonKey FROM ";
                                 String innerSelect = "(SELECT DISTINCT " + nonKeysList.get(i)+" , "+nonKeysList.get(j)+" , "+
                  		 				nonKeysList.get(l)+ ", COUNT(DISTINCT "+ nonKeysList.get(k) + ") AS ca FROM "+ schemaMap.get(tableName) + "." +tableName;
                                 String groupBy = " GROUP BY "+ nonKeysList.get(i)+" , "+nonKeysList.get(j)+" , "+nonKeysList.get(l)+ ") AS derivedTable ;";
                                 
                                 String query = select + innerSelect + groupBy;
                                 writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                                 rs = stmt5.executeQuery(query);
                                 while(rs.next()){        
                                     value1=rs.getInt("countKeyToNonKey");
                                 }
                                 ResultSet rs2= null;
                                 select = "SELECT COUNT(*) AS ck FROM ";
                                 innerSelect = "(SELECT "+nonKeysList.get(i)+","+nonKeysList.get(j)+" , "+nonKeysList.get(l)+" FROM "+
                                		 schemaMap.get(tableName) + "." + tableName;
                                 groupBy = " GROUP BY "+ nonKeysList.get(i)+","+nonKeysList.get(j)+" , "+nonKeysList.get(l)+") AS derived";
                                      
                                 query = select + innerSelect + groupBy;
                                 writeSQL(select + newLine + tab + innerSelect + newLine + tab + tab + groupBy);
                                 rs2 = stmt6.executeQuery(query);
                                 while(rs2.next()){
                                     value2=rs2.getInt("ck");
                                 }
                                 if(value1==value2){
                                     //System.out.println(keysList.get(i)+" -> "+ nonKeysList.get(j)+"\n");                                    
                                     output=output+(nonKeysList.get(i)+","+nonKeysList.get(j)+","+nonKeysList.get(l)+"->"+ nonKeysList.get(k)+", ");    
                                 }
                                 }
                             }
                         }
                     }
                 } 
             }
         }
             
         } catch(SQLException e) {
                 System.err.println("Could not check the 3NF");
                 e.printStackTrace();       
             }                   
         
         return output;
     }
	
	public static ResultSet executeQuery(String sqlStatement) throws SQLException {
//		try {
			System.out.println(sqlStatement);
			return st.executeQuery(sqlStatement);
//		} catch (SQLException e) {
//			e.printStackTrace();
//			return null;
//		}
	}
	
//	private static void verify1NF(Connection connection)
//	{
//		
//	}
	
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
	        	schemaMap.put(tableName, schemaPrivate); //***TODO: change to schemaPublic
	        	
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

}