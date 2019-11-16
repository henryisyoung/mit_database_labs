package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private TupleDesc td = null;
    private OpIterator child = null;
    private OpIterator it = null;
    private Aggregator agg = null;
    private Aggregator.Op aop = null;
    private String gFieldName = null;
    private String aFieldName = null;
    private int afield, gfield;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        if (this.aop == Aggregator.Op.SC_AVG) {
            if (this.gfield == Aggregator.NO_GROUPING)
                this.afield = 0;
            else {
                this.afield = 1;
                this.gfield = 0;
            }
        }
        TupleDesc child_td = child.getTupleDesc();

        if (child_td.getFieldType(afield) == Type.INT_TYPE) {
            this.agg = new IntegerAggregator(gfield,
                    gfield == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gfield), afield, aop);
        } else {
            this.agg = new StringAggregator(gfield,
                    gfield == Aggregator.NO_GROUPING ? null : child_td.getFieldType(gfield), afield, aop);
        }

        if (gfield == Aggregator.NO_GROUPING) {
            int nFields = 1;
            if (aop == Aggregator.Op.SUM_COUNT) {
                nFields = 2;
            }
            Type typeAr[] = new Type[nFields];
            String fields[] = new String[nFields];

            typeAr[0] = Type.INT_TYPE;//child_td.getFieldType(afield);

            if (aop == Aggregator.Op.SUM_COUNT) {
                typeAr[1] = Type.INT_TYPE;
                fields[1] = "";
            }

            fields[0] = nameOfAggregatorOp(aop) + "(" + child_td.getFieldName(afield) + ")";
            aFieldName = fields[0];

            td = new TupleDesc(typeAr, fields);
        } else {
            int nFields = 2;
            if (aop == Aggregator.Op.SUM_COUNT) {
                nFields = 3;
            }

            Type typeAr[] = new Type[nFields];
            String fields[] = new String[nFields];

            typeAr[0] = child_td.getFieldType(gfield);
            typeAr[1] = Type.INT_TYPE;//child_td.getFieldType(afield);
            fields[0] = child_td.getFieldName(gfield);
            gFieldName = fields[0];

            fields[1] = nameOfAggregatorOp(aop) + "(" + child_td.getFieldName(afield) + ")";

            if (aop == Aggregator.Op.SUM_COUNT) {
                typeAr[2] = Type.INT_TYPE;
                fields[2] = "";
            }

            aFieldName = fields[1];

            td = new TupleDesc(typeAr, fields);
        }
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return gFieldName;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return aFieldName;
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();
        if (it != null)
            it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (it == null) {
            while (child.hasNext()) {
                Tuple t = child.next();
                agg.mergeTupleIntoGroup(t);
            }

            it = agg.iterator();
            it.open();
        }

        if (it.hasNext())
            return it.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void close() {
        super.close();
        child.close();
        if (it != null) {
            it.close();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}
