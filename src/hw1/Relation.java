//Jieping Luo
package hw1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> list = new ArrayList<>();

		for (Tuple t: this.tuples){
			if(t.getField(field).compare(op, operand)){
				list.add(t);
			}
		}

		return new Relation(list, this.td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		Type[] Types = new Type[this.td.numFields()];
		String[] Fields = new String[this.td.numFields()];

		for(int i = 0; i < this.td.numFields(); i++){
			Types[i] = this.td.getType(i);
			Fields[i] = this.td.getFieldName(i);
		}

		for(int i = 0; i < fields.size(); i++){
			if(Arrays.asList(Fields).contains(names.get(i))){  //Check if each name in the "names" list is present in the "Fields" array
				throw new IllegalArgumentException();
			}

			if(!names.get(i).equals("")){
				Fields[fields.get(i)] = names.get(i); //update the "Fields" array based on the non-empty names in the "names" list
			}
		}

		return new Relation(this.tuples, new TupleDesc(Types, Fields));
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields){
		//your code here
		Type[] newTypes = new Type[fields.size()];
		String[] newFields = new String[fields.size()];

		TupleDesc newTD = new TupleDesc(newTypes, newFields);
		ArrayList<Tuple> newList = new ArrayList<>();

		for(int i = 0; i < fields.size(); i++){
			try{
//				if(fields.get(i) > td.numFields()-1){
//					throw new IllegalAccessException();
//				}
				newTypes[i] = this.td.getType(fields.get(i));
				newFields[i] = this.td.getFieldName(fields.get(i));
			}
			catch(NoSuchElementException e){
				try {
					throw new IllegalAccessException();
				} catch (IllegalAccessException ex) {
					throw new RuntimeException(ex);
				}
			}
		}

		if(newTD.getSize() == 0){
			return new Relation(newList, newTD);
		}


		for(Tuple t: this.tuples){
			Tuple tu = new Tuple(newTD);
			for(int i = 0; i < fields.size(); i++){
				tu.setField(i, t.getField(fields.get(i)));
			}
			newList.add(tu);
		}

		return new Relation(newList, newTD);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		int numFields1 = this.td.numFields();
		int numFields2 = other.td.numFields();
		Type[] types = new Type[numFields1 + numFields2];
		String[] fields = new String[numFields1 + numFields2];
		ArrayList<Tuple> newList = new ArrayList<>();

		for (int i = 0; i < numFields1; i++){
			fields[i] = this.td.getFieldName(i);
			types[i] = this.td.getType(i);
		}

		for (int j = 0; j < numFields2; j++){
			fields[j + numFields1] = other.td.getFieldName(j);
			types[j + numFields1] = other.td.getType(j);
		}

		for(int i = 0; i < this.tuples.size(); i++){
			for(int j = 0; j < other.tuples.size(); j++){
				if(this.tuples.get(i).getField(field1).equals(other.tuples.get(j).getField(field2))){
					Tuple t = new Tuple(new TupleDesc(types, fields));
					for(int k = 0; k < this.getDesc().numFields(); k++){
						t.setField(k, this.getTuples().get(i).getField(k));
					}
					for(int l = 0; l < other.getDesc().numFields(); l++){
						t.setField(l + this.getDesc().numFields(), other.getTuples().get(j).getField(l));
					}
					newList.add(t);
				}
			}
		}

		return new Relation(newList, new TupleDesc(types, fields));
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator agg = new Aggregator(op, groupBy, this.td);

		for(Tuple t: tuples){
			agg.merge(t);
		}
		return new Relation(agg.getResults(), this.td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		StringBuilder s = new StringBuilder(this.td.toString() + '\n');

		for(Tuple t: this.tuples){
			s.append(t.toString()).append('\n');
		}

//		for(int i = 0; i < this.tuples.size();i++){
//			s += this.tuples.get(i).toString() + '\n';
//		}

//		for(Tuple t: this.tuples){
//			s += t.toString() + '\n';
//		}

		return s.toString();
	}

}
