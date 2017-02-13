package db.systems;

import java.util.ArrayList;
import java.util.HashMap;

public class Table
{
	String name = "";
	ArrayList<String> keys = new ArrayList<>();
	ArrayList<String> nonKeys = new ArrayList<>();
	HashMap<String, String[]> colData = new HashMap<>();
	
	public Table(String tableName, String[] columns)
	{
		this.name = tableName;
		extractColumns(columns);
	}
	
	private void extractColumns(String[] columns)
	{
		for (int i = 0; i < columns.length; i++) {
    		String col = columns[i];
    		if (col.contains("k")) {
    			String key = col.substring(0, col.indexOf("("));
    			keys.add(key);
    		} else {
    			nonKeys.add(col);
    		}
    	}
	}
}
