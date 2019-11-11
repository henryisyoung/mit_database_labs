package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final Type gbfieldtype;
    private final Op what;
    private Map<String, IntegerAggregateValue> map;

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
        this.afield = afield;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        this.map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        String tupString = "";
        if (gbfield != NO_GROUPING) {
            tupString = tup.getField(gbfield).toString();
        }
        IntegerAggregateValue value = map.getOrDefault(tupString, new IntegerAggregateValue());

        int tupAggVal = ((IntField) tup.getField(afield)).getValue();

        value.sum += tupAggVal;
        value.count++;
        value.max = Math.max(value.max, tupAggVal);
        value.min = Math.min(value.min, tupAggVal);
        if (what == Op.SC_AVG) {
            value.sumCount += ((IntField) tup.getField(afield + 1)).getValue();
        }

        map.put(tupString, value);
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
        List<Tuple> result = new ArrayList<>();
        TupleDesc td;
        int aggField = 1;

        if (gbfield == NO_GROUPING) {
            if (what==Op.SUM_COUNT)
                td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
            else
                td = new TupleDesc(new Type[] { Type.INT_TYPE });
            aggField = 0;
        } else {
            if (what==Op.SUM_COUNT)
                td = new TupleDesc(new Type[]{ gbfieldtype, Type.INT_TYPE, Type.INT_TYPE});
            else
                td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
        }

        for (String groupVal : map.keySet()) {
            Tuple tuple = new Tuple(td);
            IntegerAggregateValue aggregateValue = map.get(groupVal);
            if (gbfield != NO_GROUPING) {
                if (gbfieldtype == Type.INT_TYPE)
                    tuple.setField(0, new IntField(new Integer(groupVal)));
                else
                    tuple.setField(0, new StringField(groupVal, Type.STRING_LEN));
            }

            switch (what){
                case MIN:
                    tuple.setField(aggField, new IntField(aggregateValue.min));
                    break;
                case MAX:
                    tuple.setField(aggField, new IntField(aggregateValue.max));
                    break;
                case AVG:
                    tuple.setField(aggField, new IntField(aggregateValue.sum / aggregateValue.count));
                    break;
                case SUM:
                    tuple.setField(aggField, new IntField(aggregateValue.sum));
                    break;
                case COUNT:
                    tuple.setField(aggField, new IntField(aggregateValue.count));
                    break;
                case SUM_COUNT:
                    tuple.setField(aggField, new IntField(aggregateValue.sum));
                    tuple.setField(aggField + 1, new IntField(aggregateValue.count));
                    break;
                case SC_AVG:
                    tuple.setField(aggField, new IntField(aggregateValue.sum / aggregateValue.sumCount));
                    break;
            }
            result.add(tuple);
        }
        return new TupleIterator(td, Collections.unmodifiableList(result));
    }

    class IntegerAggregateValue {
        private Integer max, min, sum, count, sumCount;

        public IntegerAggregateValue(){
            this.count = 0;
            this.sumCount = 0;
            this.min = Integer.MAX_VALUE;
            this.max = Integer.MIN_VALUE;
            this.sum = 0;
        }
    }
}
