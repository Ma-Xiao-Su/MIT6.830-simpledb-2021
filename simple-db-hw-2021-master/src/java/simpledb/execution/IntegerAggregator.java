package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbField;

    private Type gbFieldType;

    private int aField;

    private Op op;

    private AggregationType aggregationType;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = what;
        this.aggregationType = new AggregationType();
        switch (what) {
            case MIN :
                this.aggregationType.setStrategy(new MIN());
                break;
            case MAX :
                this.aggregationType.setStrategy(new MAX());
                break;
            case AVG :
                this.aggregationType.setStrategy(new AVG());
                break;
            case SUM :
                this.aggregationType.setStrategy(new SUM());
                break;
            case COUNT :
                this.aggregationType.setStrategy(new COUNT());
                break;
            default :
                throw new UnsupportedOperationException("Not implement");

        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField;
        if(gbField == NO_GROUPING) {
            gField = null;
        } else {
            gField = tup.getField(gbField);
        }
        aggregationType.strategy.strategyMethod(aggregationType.groupResult, gField, tup.getField(aField));
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc aggTd;
        if (gbField == NO_GROUPING) {
            aggTd = new TupleDesc(new Type[]{ Type.INT_TYPE },
                                new String[]{"aggregateVal"});
        } else {
            aggTd = new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE},
                                new String[]{"groupVal", "aggregateVal"});
        }

        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry : aggregationType.groupResult.entrySet()) {
            Tuple newTuple = new Tuple(aggTd);
            if (gbField == NO_GROUPING) {
                newTuple.setField(0, new IntField(entry.getValue()));
            } else {
                newTuple.setField(0, entry.getKey());
                newTuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(newTuple);
        }

        return new TupleIterator(aggTd, tuples);
    }


    private interface Strategy {
        void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField);
    }

    private class AggregationType {
        Strategy strategy;

        HashMap<Field, Integer> groupResult;

        AggregationType() {
            groupResult = new HashMap<>();
        }

        void setStrategy(Strategy strategy) {
            this.strategy = strategy;
        }
    }

    private class COUNT implements Strategy {
        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField) {
            int count = groupResult.getOrDefault(gbField, 0) + 1;
            groupResult.put(gbField, count);
        }
    }

    private class SUM implements Strategy {
        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField) {
            int sum = groupResult.getOrDefault(gbField, 0) + ((IntField)aField).getValue();
            groupResult.put(gbField, sum);
        }
    }

    private class AVG implements Strategy {
        private HashMap<Field, Integer> sum = new HashMap<>();
        private HashMap<Field, Integer> count = new HashMap<>();

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField) {
            int groupSum = sum.getOrDefault(gbField, 0) + ((IntField)aField).getValue();
            sum.put(gbField, groupSum);
            int groupCount = count.getOrDefault(gbField, 0) + 1;
            count.put(gbField, groupCount);
            groupResult.put(gbField, groupSum / groupCount);
        }
    }
    private class MIN implements Strategy {
        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField) {
            int minValue = Math.min(((IntField)aField).getValue(), groupResult.getOrDefault(gbField, Integer.MAX_VALUE));
            groupResult.put(gbField, minValue);
        }
    }

    private class MAX implements Strategy {
        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field gbField, Field aField) {
            int maxValue = Math.max(((IntField)aField).getValue(), groupResult.getOrDefault(gbField, Integer.MIN_VALUE));
            groupResult.put(gbField, maxValue);
        }
    }
}
