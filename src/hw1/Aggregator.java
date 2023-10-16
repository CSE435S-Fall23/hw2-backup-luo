package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {

	private AggregateOperator o;
	private boolean groupBy;
	private TupleDesc td;
	private ArrayList<Tuple> tuples;
	private HashMap<IntField, Integer> counts = new HashMap<>();
	private IntField countField;
	private HashMap<Field, Field> map = new HashMap<>();

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
		this.tuples = new ArrayList<Tuple>();

		tuples.add(new Tuple(td));
		countField = new IntField(0);
		counts.put(countField, 0);

		switch(o){
			case MAX:
				if(td.getType(0) == Type.INT){
					this.tuples.get(0).setField(0, new IntField(Integer.MIN_VALUE));
				}
				if(td.getType(0) == Type.STRING) {
					this.tuples.get(0).setField(0, new StringField("a"));
				}
				break;
			case MIN:
				if(td.getType(0) == Type.INT){
					this.tuples.get(0).setField(0, new IntField(Integer.MAX_VALUE));
				}
				if(td.getType(0) == Type.STRING) {
					this.tuples.get(0).setField(0, new StringField("z"));
				}
				break;
			case SUM:
			case AVG:
			case COUNT:
				this.tuples.get(0).setField(0, new IntField(0));
				break;
			/* default: */
		}
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		switch(o){
			case MAX:
				if(!this.tuples.get(0).getField(0).compare(RelationalOperator.GT, t.getField(0))) {
					this.tuples.get(0).setField(0, t.getField(0));
				}
				break;
			case MIN:
				if(this.tuples.get(0).getField(0).compare(RelationalOperator.GT, t.getField(0))) {
					this.tuples.get(0).setField(0, t.getField(0));
				}
				break;
			case SUM:
			case AVG:
				IntField sum = (IntField) this.tuples.get(0).getField(0);
				this.tuples.get(0).setField(0, new IntField(sum.getValue() + ((IntField) t.getField(0)).getValue()));
				counts.put(countField, counts.get(countField) + 1);
				break;
			case COUNT:
				IntField times = (IntField) this.tuples.get(0).getField(0);
				this.tuples.get(0).setField(0, new IntField(times.getValue()+1));
				break;
		}
		if(this.groupBy) {
			mergeGroup(t);
			return;
		}
	}

	private void mergeGroup(Tuple t) {
		Field key =t.getField(0);
		IntField sum = (IntField) map.getOrDefault(key, new IntField(0));
		int times = counts.getOrDefault(key, 0);

		switch(o){
			case MAX:
				if(map.getOrDefault(key, null) == null || !map.get(key).compare(RelationalOperator.GT, t.getField(1))) {
					map.put(key, t.getField(1));
				}
				break;
			case MIN:
				if(map.getOrDefault(key, null) == null || map.get(key).compare(RelationalOperator.GT, t.getField(1))) {
					map.put(key, t.getField(1));
				}
				break;
			case SUM:
			case AVG:
				map.put(key, new IntField(sum.getValue() + ((IntField)t.getField(1)).getValue()));
				counts.put((IntField) key, counts.getOrDefault(key, 0) + 1);
				break;
			case COUNT:
				counts.put((IntField) t.getField(0), times + 1);
				break;
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		IntField sum = (IntField) this.tuples.get(0).getField(0);
		int count = counts.get(countField);

		if(this.groupBy) {
			return this.getResultsGroup();
		}
		if(o.equals(AggregateOperator.AVG)) {
			this.tuples.get(0).setField(0, new IntField(sum.getValue() / count));
		}
		return this.tuples;
	}
	private ArrayList<Tuple> getResultsGroup(){
		this.tuples.clear();
		for(Field f : map.keySet()) {
			Tuple t = new Tuple(this.td);
			if(o.equals(AggregateOperator.AVG)) {
				t.setField(0, f);
				t.setField(1, new IntField(((IntField)map.get(f)).getValue() / counts.get(f)));
			}else {
				t.setField(0, f);
				t.setField(1, map.get(f));
			}
			this.tuples.add(t);
		}
		return this.tuples;
	}
		//return null;

}
