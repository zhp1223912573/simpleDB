package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.math.BigInteger;
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

    //当前页面的id号
    final HeapPageId pid;
    //当前页面的模式
    final TupleDesc td;
    //头部--所有元组的位图 对应位为1时说明该元组存在，为0则不存在
    final byte[] header;
    //存放元组条信息
    final Tuple[] tuples;
    //当前元组的总数
    final int numSlots;
    //调用该页进行活动的事务id
    private TransactionId tid;

    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

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
        // some code goes here
        //若不为0
       if(numSlots!=0) return numSlots;
        //为则按照公式计算出一页内能容纳的tuples以及其位图的数量
        //_tuples per page_ = floor((_page size_ * 8) / (_tuple size_ * 8 + 1))
        int numTuples = (BufferPool.getPageSize()*8)/(td.getSize()*8+1);
        return numTuples;

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {        
        
        // some code goes here
        //headerBytes = ceiling(tupsPerPage/8)
        int headerSize = (int)Math.ceil(getNumTuples()/8.0);
        return headerSize;
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
    // some code goes here
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
        for (byte b : header) {
            try {
                dos.writeByte(b);
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
        // some code goes here
        // not necessary for lab1
        if(t==null) throw new NoSuchElementException();
        RecordId tRecord = t.getRecordId();

        if(!(tRecord.getPageId().equals(this.getId()))){
            throw new DbException("元组不在该页面中");
        }

        if(!(isSlotUsed(tRecord.getTupleNumber()))){
            throw new DbException("元组所在的slot已经为空，无法删除");
        }

        //直接使对应位置的header设置为未使用
        markSlotUsed(tRecord.getTupleNumber(), false);
        tuples[tRecord.getTupleNumber()]=null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if(t==null) throw new NoSuchElementException("传入的元组为空");

        if(getNumEmptySlots()==0) throw new DbException("此页面中元组数量已满，无法插入新的元组");

        if(!t.getTupleDesc().equals(td)) throw new DbException("插入元组类型不符");

        //获取第一个为空的位置 将其加入元组
       int firstEmpty = getNextEmptySlot();
       markSlotUsed(firstEmpty, true);
       tuples[firstEmpty] = t;
       //为当前新加入的元组设置RecordId
        RecordId recordId =  new RecordId(pid, firstEmpty);
        t.setRecordId(recordId);

    }

    /**获取第一个为空的元组位置
     * @return
     */
    private int getNextEmptySlot() throws DbException {
        for(int i=0;i<header.length*8;i++){
            if(!isSlotUsed(i)){
                return i;
            }
        }
        throw new DbException("不存在空的位置存放元组");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	    // not necessary for lab1
        if (dirty){
            this.tid=tid;
        }
        else{
            this.tid=null;
        }


    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        return tid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptySlots=0;
        for(int i=0;i<getNumTuples();i++){
            if(!isSlotUsed(i)) emptySlots++;
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
            // some code goes here
            //判断第i个插槽是否以及被使用 根据header表示的位图进行判断
        /*每个字节的低位（最低有效位）代表文件中较早的插槽的状态。
        因此，第一个字节的最低位表示页面中的第一个插槽是否正在使用。
        第一个字节的第二低位表示页面中的第二个插槽是否正在使用，依此类推
        。另外，请注意，最后一个字节的高位可能与文件中实际存在的插槽不对应，
        因为插槽的数量可能不是8的倍数。还要注意，所有Java虚拟机都是big-endian。*/

            int indexInbyet = i/8;//判断位于header的第几个字节
            int location = i%8;//位于字节从右往左的第几位
            //将indexInbyte位置的byte与掩码1左移location位后的数字相与
            // 若该数为0说明改为不为1 为false
            //代码错误 无法检测出id编号的slot是否使用
//        int i1 = header[indexInbyet] & (1 << location);
//        if(i1==0) return false;
//        return true;
            byte target = header[indexInbyet];
            return (byte)(target<<(7-location))<0;
    }



    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new TupleIterator();
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     * 根据传入的value值将header中第i个位的信息进行修改。
     * true--使用（插入操作）
     * false--未使用（删除操作）
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1
        //老样子 获取该元组在header中的第几个字节的第几位
        int indexOfAll = i/8;
       int indexOfByte =i%8;
       //将要修改的byte转化为BigInteger类型
        byte[] bytes1 =new byte[]{header[indexOfAll]};
        BigInteger bigInteger = new BigInteger(bytes1);
       if(value){
           bigInteger=bigInteger.setBit(indexOfByte);
       }else{
           bigInteger=bigInteger.clearBit(indexOfByte);
       }

        byte[] bytes = bigInteger.toByteArray();
       byte changeByte = bytes[bytes.length-1];
       header[indexOfAll]=changeByte;

//        int whichByte = i / 8;
//        int whichBit = i % 8;
//        byte[] byteArContainsI = new byte[] {header[whichByte]};
//        BigInteger bi = new BigInteger(byteArContainsI);
//        if (value) {
//            bi=bi.setBit(whichBit);
//        } else{
//            bi=bi.clearBit(whichBit);
//        }
//        byte [] changedbyteArContainsI = bi.toByteArray();
//        byte changedByte=changedbyteArContainsI[changedbyteArContainsI.length-1];
//        header[whichByte]=changedByte;

    }

    // tuple迭代器 参考别人的
    private class TupleIterator implements Iterator<Tuple>{
        //当前遍历到的slot位置（未必都是tuple）
        private int index = 0;
        //当前遍历到的已经访问过的tuple数量 避免当tuple数组中非空元组访问完后继续访问
        private int visited=0;
        //当前页面的所有非空元组数量
        private int tupleUsedNum=getNumTuples()-getNumEmptySlots();

        @Override
        public boolean hasNext() {
            return index<getNumTuples()&&visited<tupleUsedNum;
        }

        @Override
        public Tuple next() {
            if(!hasNext()){
                throw new NoSuchElementException("不存在该元组");
            }
            //找到非空的元组
            while(!isSlotUsed(index)){
                index++;
            }

            //返回一个非空元组 都要usednum++
            visited++;

            //index需要自增，避免一直访问自己
            return tuples[index++];
        }
    }
}
