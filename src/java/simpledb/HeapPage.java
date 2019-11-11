package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    private Map<RecordId, Integer> tuplesMap;
    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);
    private volatile boolean dirty;
    private volatile TransactionId dirtierId;
    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
        this.tuplesMap = new HashMap<>();
        this.dirty = false;
        this.dirtierId = null;

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        double tuplesheaderbits = td.getSize() * 8 + 1;

        // the double here is very important, or it may return a wrong result.
        int NumTuples = (int) Math.floor((double) BufferPool.getPageSize() * 8 / tuplesheaderbits);

        return NumTuples;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        return (int) Math.ceil(getNumTuples() / 8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        RecordId rid = t.getRecordId();
        if ((rid.getPageId().getPageNumber() != pid.getPageNumber()) || (rid.getPageId().getTableId() != pid.getTableId()))
            throw new DbException("tried to delete tuple on invalid page or table");
        if (!isSlotUsed(rid.getTupleNumber()))
            throw new DbException("tried to delete null tuple.");
        markSlotUsed(rid.getTupleNumber(), false);
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if (!t.getTupleDesc().equals(td))
            throw new DbException("type mismatch, in addTuple");

        int goodSlot = -1;
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i) && goodSlot == -1) {
                goodSlot = i;
                break;
            }
        }

        if (goodSlot == -1)
            throw new DbException("called addTuple on page with no empty slots.");

        markSlotUsed(goodSlot, true);
        Debug.log(1, "HeapPage.addTuple: new tuple, tableId = %d pageId = %d slotId = %d", pid.getTableId(),
                pid.getPageNumber(), goodSlot);
        RecordId rid = new RecordId(pid, goodSlot);
        t.setRecordId(rid);
        tuples[goodSlot] = t;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty = dirty;
        this.dirtierId = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        return dirtierId;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int NumEmptySlots = 0;
        for (int i = 0; i < this.numSlots; i++) {
            if (!isSlotUsed(i))
                NumEmptySlots++;
        }
        return NumEmptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int byteNum = i / 8;//计算在第几个字节
        int posInByte = i % 8;//计算在该字节的第几位,从右往左算（这是因为JVM用big-ending）
        return isOne(header[byteNum], posInByte);
    }

    /**
     * @param target    要判断的bit所在的byte
     * @param posInByte 要判断的bit在byte的从右往左的偏移量，从0开始
     * @return target从右往左偏移量pos处的bit是否为1
     */
    private boolean isOne(byte target, int posInByte) {
        // 例如该byte是11111011,pos是2(也就是0那个bit的位置)
        // 那么只需先左移7-2=5位即可通过符号位来判断，注意要强转
        return (byte) (target << (7 - posInByte)) < 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int byteNum = i / 8;//计算在第几个字节
        int posInByte = i % 8;//计算在该字节的第几位,从右往左算（这是因为JVM用big-ending）
        header[byteNum] = editBitInByte(header[byteNum], posInByte, value);
    }

    /**
     * 修改一个byte的指定位置的bit
     * @param target    待修改的byte
     * @param posInByte bit的位置在target的偏移量，从右往左且从0开始算，取值范围为0到7
     * @param value     为true修改该bit为1,为false时修改为0
     * @return 修改后的byte
     */
    private byte editBitInByte(byte target, int posInByte, boolean value) {
        if (posInByte < 0 || posInByte > 7) {
            throw new IllegalArgumentException();
        }
        byte b = (byte) (1 << posInByte);//将1这个bit移到指定位置，例如pos为3,value为true，将得到00001000
        //如果value为1,使用字节00001000以及"|"操作可以将指定位置改为1，其他位置不变
        //如果value为0,使用字节11110111以及"&"操作可以将指定位置改为0，其他位置不变
        return value ? (byte) (target | b) : (byte) (target & ~b);
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        List<Tuple> temp = new ArrayList<>();
        for (int i = 0; i < this.getNumTuples(); i++) {
            if (isSlotUsed(i))
                temp.add(tuples[i]);
        }
        return temp.iterator();
    }

}

