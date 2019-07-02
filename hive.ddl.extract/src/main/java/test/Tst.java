package test;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Iterables;

public class Tst {
	
	public static void main(String[] args) {
		
	List<Integer>intList = new ArrayList<Integer>();
	
	
	for(int i =0;i<5003;i++){
		intList.add(i);
	}
		
	Iterable<List<Integer>> parts = Iterables.partition(intList, 1000);
	
	System.out.println(Iterables.size(parts));
	 
	for(List<Integer> part : parts){
		
		System.out.println(part.size());
		
	}
	 
	 
	 
	}

}
