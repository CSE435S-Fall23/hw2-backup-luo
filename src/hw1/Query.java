package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;

	public Query(String q) {
		this.q = q;
	}

	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();

		//your code here
		List<Join> joins = sb.getJoins();
		Catalog catalog = Database.getCatalog();
		FromItem fromItem = sb.getFromItem();

		String tableName = fromItem.toString();
		if (fromItem.getAlias() != null) {
			tableName = fromItem.getAlias().getName();
		}

		int tableId = catalog.getTableId(fromItem.toString());
		ArrayList<Tuple> tuples = catalog.getDbFile(tableId).getAllTuples();
		TupleDesc td = catalog.getTupleDesc(tableId);
		Relation rel = new Relation(tuples, td);
		List<SelectItem> items = sb.getSelectItems();

		if(joins != null){
			for(Join join: joins){

				tableName = join.getRightItem().toString();
				if (join.getRightItem().getAlias() != null) {
					tableName = join.getRightItem().getAlias().getName();
				}

				int id = catalog.getTableId(join.getRightItem().toString());
				ArrayList<Tuple> tu = catalog.getDbFile(id).getAllTuples();
				TupleDesc tdId = catalog.getTupleDesc(id);
				Relation relation = new Relation(tu, tdId);
				String[] sl= join.getOnExpression().toString().split(" = ")[0].split("\\.");
				String[] sr= join.getOnExpression().toString().split(" = ")[1].split("\\.");
				int l = 0;
				int r = 0;
				rel = rel.join(relation,l,r);
				for(int i = 0; i < rel.getDesc().numFields(); i++){
					if(rel.getDesc().getFieldName(i).equalsIgnoreCase(sl[1])){
						l = i;
					}
					if(rel.getDesc().getFieldName(i).equalsIgnoreCase(sr[1])){
						l = i;
					}
				}
				for(int i = 0; i < tdId.numFields();i++){
					if(tdId.getFieldName(i).equalsIgnoreCase(sl[1])){
						r = i;
					}
					if(tdId.getFieldName(i).equalsIgnoreCase(sr[1])){
						r = i;
					}
				}
			}
		}

		WhereExpressionVisitor wev = new WhereExpressionVisitor();
		if(sb.getWhere() != null){
			sb.getWhere().accept(wev);
			rel = rel.select(td.nameToId(wev.getLeft()), wev.getOp(), wev.getRight());
		}

		ArrayList<Integer> colId = new ArrayList<>();
		ColumnVisitor cv = new ColumnVisitor();
		if(!items.get(0).toString().equals("*")){
			if(items.get(0).toString().equals("COUNT(*)")){
				colId.add(0);
				rel = rel.project(colId);
				rel = rel.aggregate(AggregateOperator.COUNT, false);
				return  rel;
			}else{
				for(SelectItem selectItem: items){
					selectItem.accept(cv);
					for(int i = 0; i < rel.getDesc().numFields(); i++){
						if(rel.getDesc().getFieldName(i).equals(cv.getColumn())){
							colId.add(i);
						}
					}

				}
				rel = rel.project(colId);


				ArrayList<Integer> fieldsToRename = new ArrayList<>();
				ArrayList<String> newNames = new ArrayList<>();


				// Handling column alias
				for(SelectItem selectItem : items){

					if (selectItem instanceof net.sf.jsqlparser.statement.select.SelectExpressionItem) {
						net.sf.jsqlparser.statement.select.SelectExpressionItem sei = (net.sf.jsqlparser.statement.select.SelectExpressionItem) selectItem;
						if (sei.getAlias() != null) {
							String originalColumnName = sei.getExpression().toString();
							String aliasColumnName = sei.getAlias().getName();

							// Find the index of the original column name in the relation
							int columnIndex = -1;
							for (int i = 0; i < rel.getDesc().numFields(); i++) {
								if (rel.getDesc().getFieldName(i).equals(originalColumnName)) {
									columnIndex = i;
									break;
								}
							}

							if (columnIndex != -1) {
								fieldsToRename.add(columnIndex);
								newNames.add(aliasColumnName);
							}
						}
					}


					selectItem.accept(cv);
					List<Expression> list = sb.getGroupByColumnReferences();
					if(cv.isAggregate()){
						if(list != null){
							rel = rel.aggregate(cv.getOp(), true);
						}else{
							rel = rel.aggregate(cv.getOp(), false);
						}
					}
				}
				// Renaming columns after accumulating all changes
				if (!fieldsToRename.isEmpty() && !newNames.isEmpty()) {
					rel = rel.rename(fieldsToRename, newNames);
				}
			}
		}
		return rel;
	}
}
