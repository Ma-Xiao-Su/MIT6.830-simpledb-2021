package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupField;

    private Type groupFieldType;

    private int aggregatorField;

    private Op op;

    private Map<Field, Integer> groupResult;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupField = gbfield;
        this.groupFieldType = gbfieldtype;
        this.aggregatorField = afield;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Not support");
        }
        this.op = what;
        this.groupResult = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field field = tup.getField(groupField);
        int count = groupResult.getOrDefault(field, 0);
        ++count;
        groupResult.put(field, count);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        TupleDesc aggTd;
        if (groupField==NO_GROUPING) {
            aggTd = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{"aggregateVal"});
        } else {
            aggTd = new TupleDesc(new Type[]{groupFieldType, Type.INT_TYPE},
                    new String[]{"groupVal", "aggregateVal"});
        }
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry:groupResult.entrySet()) {
            Tuple newTuple = new Tuple(aggTd);
            if (groupField==NO_GROUPING)
                newTuple.setField(0, new IntField(entry.getValue()));
            else {
                newTuple.setField(0, entry.getKey());
                newTuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(newTuple);
        }
        return new TupleIterator(aggTd, tuples);
//        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
