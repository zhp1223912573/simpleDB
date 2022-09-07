package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    //表示当前记录条属于的页面
    private PageId pageId;
    //表示当前的记录条的编号
    private int tupleNo;

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
        // some code goes here
        pageId=pid;
        tupleNo=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
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
        // some code goes here
        //如果当前记录条与o的pageId相同，并且记录条编号也相同则两者相等
        if(o==null){
            return false;
        }

        if(this==o) return true;

        if(o instanceof RecordId){
            RecordId recordId = (RecordId)o;
            return this.pageId.equals(recordId.pageId)
                    &&this.tupleNo==recordId.tupleNo;
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
        // some code goes here
        StringBuilder sb = new StringBuilder();
        sb.append(pageId).append(tupleNo);
        return sb.toString().hashCode();

    }

}
