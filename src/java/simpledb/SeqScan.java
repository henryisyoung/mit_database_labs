package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private Integer tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private TupleDesc td;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        reset(tableid, tableAlias);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableAlias = tableAlias;
        this.tableId = tableid;
        this.iterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        updateTupleDesc();
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        iterator.open();
        updateTupleDesc();
    }

    private void updateTupleDesc() {
        int tdlength = Database.getCatalog().getTupleDesc(tableId).numFields();
        String[] fieldAr = new String[tdlength];
        Type[] typeAr = new Type[tdlength];
        for(int i=0 ; i < tdlength;i++) {
            String fieldName = Database.getCatalog().getTupleDesc(tableId).getFieldName(i);

            fieldAr[i] = tableAlias + "." + fieldName;
            typeAr[i] = Database.getCatalog().getTupleDesc(tableId).getFieldType(i);
        }
        this.td = new TupleDesc(typeAr,fieldAr);
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        return iterator.next();
    }

    public void close() {
        iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iterator.rewind();
    }
}
