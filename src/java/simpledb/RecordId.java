package simpledb;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pageId;
    private final Integer tupleNO;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pageId = pid;
        this.tupleNO = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return tupleNO;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        return pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(!(o instanceof RecordId))
            return false;

        RecordId temp = (RecordId) o;
        if(this.hashCode() == temp.hashCode()) {
            return true;
        }

        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        int hashCode = pageId.hashCode();

        int a = 31;
        int BIG_PRIME = 111236313;
        byte[] bytes = ByteBuffer.allocate(4).putInt(getTupleNumber()).array();

        for (Byte b : bytes){
            hashCode = (b * a + hashCode) % BIG_PRIME;
        }
        return hashCode;
    }

}
