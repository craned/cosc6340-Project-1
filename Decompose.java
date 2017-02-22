package database;

import java.awt.event.KeyListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
public class Decompose {
	String s="k1->a,k2->b,a->b";
	HashMap<String, String> dep=new HashMap<String,String>();
	HashMap<String, ArrayList<ArrayList<String>>> decompose=new HashMap<String, ArrayList<ArrayList<String>>>();
	ArrayList<String> mainkey=new ArrayList<String>();
	ArrayList<String> mainvalue=new ArrayList<String>();
	ArrayList<String> keyList=new ArrayList<String>();
	ArrayList<String> nonkeyList=new ArrayList<String>();
	String maintablename="";
	
	public void nf3(){
		keyList.add("k1");
		keyList.add("k2");
		//keyList.add("c");
		nonkeyList.add("a");
		nonkeyList.add("b");
		//nonkeyList.add("d");
		String[] s1=s.split(",");
		/*for(int i=0; i<s1.length;i++){
			String k=s1[0];
			String v=s1[3];
			if(!dep.containsKey(k))
				dep.put(k, v);
		}*/
		dep.put("k1", "a");
		dep.put("k2", "b");
		//dep.put("c", "d");
		//dep.put("k1", "a");
		//dep.put("a", "b");
		//dep.put("b", "c");
		int count=2;
		for(Map.Entry<String,String> entry:dep.entrySet()){
			ArrayList<ArrayList<String>> tem=new ArrayList<ArrayList<String>>();
			ArrayList<String> temKey=new ArrayList<String>();
			ArrayList<String> temValue=new ArrayList<String>();
			temKey.add(entry.getKey());
			temValue.add(entry.getValue());
			tem.add(temKey);
			tem.add(temValue);
			decompose.put("R"+Integer.toString(count),tem);
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
		temValue.addAll(nonkeyList);
		temValue.retainAll(mainkey);
		System.out.println(temValue);
		tem.add(keyList);
		tem.add(temValue);
		decompose.put("R1", tem);
		String sc1="create table";
		String s2="";
		/*for(int i=0;i<keyList.size();i++){
		s2=s2+keyList.get(i).toString()+" varchar(10) ,";
		}
		for(int i=0;i<nonkeyList.size();i++){
			s2=s2+nonkeyList.get(i).toString()+" varchar(10) ,";
			}
		s2=s2+"primary key(";
		for(int i=0;i<keyList.size();i++){
			if(i<keyList.size()-1)
				s2=s2+keyList.get(i).toString()+",";
			else
				s2=s2+keyList.get(i).toString()+"))";
			}*/
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
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
			System.out.println(s11+" "+entry.getKey()+"("+s21);
			
			
			
		}
		
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			String sd1="insert into";
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
					sd3=sd3+key1.get(i).toString()+"."+maintablename+",";

			}
			
			for(int i=0;i<nonkey1.size();i++){
				if(i<nonkey1.size()-1)
				sd3=sd3+nonkey1.get(i).toString()+"."+maintablename+",";
				else
					sd3=sd3+nonkey1.get(i).toString()+"."+maintablename;
			}
			System.out.println(sd1+" "+entry.getKey()+"("+sd2+" "+"select"+sd3+"from"+" "+maintablename);;
		}
	}
	public void nf2(){
		ArrayList<String> pkey=new ArrayList<String>();
		ArrayList<String> nkey=new ArrayList<String>();
		
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			ArrayList<ArrayList<String>> values=new ArrayList<ArrayList<String>>();
			pkey.addAll(entry.getValue().get(0));
			nkey.addAll(entry.getValue().get(1));
		}
		
	}
	public void join(){
		HashMap<String, ArrayList<ArrayList<String>>> join=new HashMap<String, ArrayList<ArrayList<String>>>();
		int count=2;
		ArrayList<String> pkey=new ArrayList<String>();
		ArrayList<String> nkey=new ArrayList<String>();
		ArrayList<ArrayList<String>> values=new ArrayList<ArrayList<String>>();
		if(decompose.size()>0){
			pkey.addAll(decompose.get("R1").get(0));
			nkey.addAll(decompose.get("R1").get(1));  
			values.add(pkey);
			values.add(nkey);
			join.put("T1",values);
		for(Map.Entry<String ,ArrayList<ArrayList<String>>> entry:decompose.entrySet()){
			
			
			ArrayList<String> pkey1=new ArrayList<String>();
			ArrayList<String> nkey1=new ArrayList<String>();
			ArrayList<ArrayList<String>> values1=new ArrayList<ArrayList<String>>();
			ArrayList<String> tempkeys=new ArrayList<String>();
			
			if(entry.getKey()!="R1")
			{
			
				String s1="";
				tempkeys.addAll(join.get("T"+Integer.toString(count-1)).get(0));
				tempkeys.addAll(join.get("T"+Integer.toString(count-1)).get(1));
				if(tempkeys.containsAll(nkey1)){
					System.out.println("asdf");
						values1.add(join.get("T"+Integer.toString(count-1)).get(0));
						pkey1.addAll(entry.getValue().get(1));
						nkey.addAll(pkey1);
						values1.add(nkey);
					join.put("T"+Integer.toString(count),values1);
					System.out.println(join.get("T"+Integer.toString(count)));
					for(String x:join.get("T"+Integer.toString(count-1)).get(0)){
						s1=s1+"T"+Integer.toString(count-1)+"."+x+",";
					}
					for(int j=0;j<join.get("T"+Integer.toString(count-1)).get(1).size();j++){
				
							s1=s1+"T"+Integer.toString(count-1)+"."+join.get("T"+Integer.toString(count-1)).get(1).get(j)+",";

					}
					for(int i=0;i<entry.getValue().get(1).size();i++){
						if(i<entry.getValue().get(1).size()-1)
						s1=s1+entry.getKey()+"."+entry.getValue().get(1).get(i)+",";
						else
							s1=s1+entry.getKey()+"."+entry.getValue().get(1).get(i);
					}
					String s2="";
					for(int i=0;i<entry.getValue().get(0).size();i++){
						if(i<entry.getValue().get(1).size()-1)
						s2=s2+"T"+Integer.toString(count-1)+"."+entry.getValue().get(0).get(i)+"="+entry.getKey()+"."+entry.getValue().get(0).get(i)+"and";
						else
							s2=s2+"T"+Integer.toString(count-1)+"."+entry.getValue().get(0).get(i)+"="+entry.getKey()+"."+entry.getValue().get(0).get(i);
					}
					String s3="";
					for(int j=0;j<join.get("T"+Integer.toString(count-1)).get(0).size();j++){
						if(j<join.get("T"+Integer.toString(count-1)).get(0).size()-1)
						s3=s3+"T"+Integer.toString(count-1)+"."+join.get("T"+Integer.toString(count-1)).get(0).get(j)+",";
						else
							s3=s3+"T"+Integer.toString(count-1)+"."+join.get("T"+Integer.toString(count-1)).get(0).get(j);
					}
				System.out.println("select"+" "+s1+" from "+"T"+Integer.toString(count-1)+" inner join "+entry.getKey()+" on"+s2+" order by "+s3);
			}
		
			}
			count++;
		}
		}
	}
	public static void main(String args[])
	{
		Decompose d=new Decompose();
		d.nf3();
		d.nf2();
		d.join();
	}
}




