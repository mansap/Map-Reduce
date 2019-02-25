/**
  *  Title: MapRedClient.java
  *  
  *  Description: This class implements the mapper and reducer functions in the 
  *  			  Map Reduce paradigm. It also includes function for performing 
  *  			  post-processing operations on results obtained after all
  *  			  reducers complete their work.
  *  
  *  Along with the main() method, it has the following methods:
  *  	1. mapper()
  *  	2. reducer()
  *  	3. postprocessing()
  *
  *  	In addition to this, it also has an inner class called MyComp which 
  *     implements comparator interface, ordering elements in descending order.
  *	
  *	 MapRedClient class extends the class MapReduce	 
  *
  *  Author for MapReduce class is: Dr.Trudy Howles.
  *  Reference : https://www.cs.rit.edu/~tmh/courses/720-2016/
  *
  *  	
  *
**/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

public class MapRedClient extends MapReduce
{	
	public static void main(String[] args)
	{
		MapRedClient mr = new MapRedClient();	
		String path = args[0];		
		/* Call execute function which handles multiple mappers and reducers 
		 * for the simulator environment */
		mr.execute(path);
		mr.postprocessing();		
	}
	
	/** 
	 *  Class: MyComparator
	 *  Description: Implements the comparator interface for descending 
	 *  			 ordering of elements.
     **/
	class MyComparator implements Comparator<Integer>
	{
		@Override
	    public int compare(Integer o1, Integer o2) 
	    {
	        return o2.compareTo(o1);
	    }	
	}
	
	/** 
	 *  Method: postprocessing()
	 *  Description: Performs post-processing operations on results obtained
	 *  			 from all reducers. Post-processing is done only when all
	 *  			 reducers finish their work.
     *	
     *  @param  None
     *  
     *  @return None
     **/
	public void postprocessing()
	{
		TreeMap<Integer, String> counts = new TreeMap<Integer,String>(new MyComparator());
		TreeMap<String, HashSet<String>> accounts = new TreeMap<String, HashSet<String>>();
		int total = 0;
		int index = 1;
		int size = 0;
		int len = 0;
		int accounts_len = 0;
		
		/** Count total number of occurrences for each IP address **/	
		/* Loop over all IP addresses in the results array */
		for(Map.Entry<String,LinkedList<String>> entry : this.getResults().entrySet()) 
		{
			/* Each element in results array is a key-value pair with key 
			 * being IP address and value being a linked list for
			 * accounts accessed. 
			 * Each linked list represents output from a different reducer */
			
			StringBuffer concatSS = new StringBuffer();
			total =  entry.getValue().size();
			/*We form a single string for all accounts accessed by IP address*/
			for (String m : entry.getValue())
			{
				concatSS.append(m);
			}
			String concat = concatSS.toString();
			String[] splitS = concat.split(" ");
			/* Length of this string array gives us total number of occurrences 
			 * for this IP address */
			total = splitS.length;
			String kk = entry.getKey();
			counts.put(total,kk);
			
        	HashSet<String> uniqueAcnts = new HashSet<String>();
        	for (int i = 0; i < splitS.length; i++)
        	{
        		uniqueAcnts.add(splitS[i]);
        	}

	        accounts.put(kk, uniqueAcnts);
		}
		if (counts.entrySet().size() >= 15)
		{
			size = 15;
		}
		else
		{
			size = counts.entrySet().size();
		}
		
		for(Map.Entry<Integer,String> entry : counts.entrySet())
		{
			System.out.println("#" + index + " occurring address:\t" + entry.getValue());
			index++;
			System.out.print("\t"); 
			accounts_len = accounts.get(entry.getValue()).size();
			len = 0;
			for (String g : accounts.get(entry.getValue()))
			{				
				len++;
				if(len == accounts_len)
				{
					System.out.print(g.substring(1) + " ");
				}
				else
				{
					System.out.print(g.substring(1) + ", ");
				}
			}
			System.out.println();
			System.out.println();
			size --;
			if (size <= 0)
			{
				break;
			}
		}		
	}
	
	
	/** 
	 *  Method: mapper()
	 *  Description: This method implements the mapper method of its parent 
	 *  			 class 'MapRed.class' in the Map Reduce paradigm. 
	 *  			 It calls emit_intermediate method from 'Mapred.class' 
	 *  			 to emit a key value pair.
	 *  			 Here, key is the IP address and value is the account name.
	 *  			 All results emitted by reducer are stored in the 'temp'
	 *  			 array of 'MapRed.class'
	 *
     *  @param var1 - String containing the path of file or filename
     *  
     *  @throws Exception - If input provided is not a valid file path
     *  @return None
     **/
	@Override
	public void mapper(String var1) 
	{
		File corpus = new File(var1);
		File[] listFiles = corpus.listFiles();
		for (File f: listFiles)
		{
			try
			{
				FileReader fr = new FileReader(f);
				BufferedReader br = new BufferedReader(fr);						
				String line = br.readLine();
				while ( line != null)
				{					
					String[] parts = line.split(" ");
					String key_IP = parts[0];
					String[] parts1 = line.split("/");
					String value = "";					
					if(parts1.length > 3){
					    value = parts1[3];
					}
					line = br.readLine();
					if(value.startsWith("~"))
					{
						this.emit_intermediate(key_IP, value);
					}					
				}	
				br.close();
			}
			catch(Exception exp)
			{
				exp.printStackTrace();
			}			
		}	
	}
	
	/** 
	 *  Method: reducer()
	 *  Description: This method implements the reducer method of its parent 
	 *  			 class 'MapRed.class' in the Map Reduce paradigm. 
	 *  			 It calls the emit function to emit a key value pair.
	 *  			 Here, key is the IP address and value is the account name.
	 *  			 All results emitted by reducer are stored in the 'results'
	 *  			 array of 'MapRed.class'
	 *
     *  @param var1 - String containing IP address, 
     *  			  this is key in the key-value pair
     *  @param var2 - Linked List of Strings containing corresponding account 
     *  			  names, this is value in the key-value pair
     *  
     *  @return None
     **/

	@Override
	public void reducer(String var1, LinkedList<String> var2) 
	{     
		  StringBuffer ss = new StringBuffer();
		  for(String s : var2)
		  {
			  ss.append(s + " ");
		  }
		  String result = ss.toString();	
		  this.emit(var1, result); 		
	}	
}
