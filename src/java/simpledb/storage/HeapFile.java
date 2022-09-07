package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    //HeapFile保存的文件
    private File file;
    //该文件的表的描述符
    private TupleDesc tupleDesc;
    //代表文件中保存的页数
    private int pageNum;
    //记录文件是否第一打开
    //private boolean firstOpen;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        tupleDesc = td;
        //文件页数大小等于 文件大小与页尺寸之比
        pageNum = (int)file.length()/BufferPool.getPageSize();
       // firstOpen =true;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
       return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**参考别人的 对文件读取不是很懂
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        Page page = null;
        byte data[] =new byte[BufferPool.getPageSize()];

        //随机访问file文件并读出
        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
            //根据要读去的页面号 获取在文件中的位置
            int pos = pid.getPageNumber()*BufferPool.getPageSize();
            //设置指针位置
            raf.seek(pos);
            raf.read(data,0,data.length);
            page = new HeapPage((HeapPageId) pid,data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    //将page写入磁盘中
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        if(page==null) throw  new NullPointerException("页面不存在！");


        //获取要写入的页的data数据 根据其页号写入文件所在位置的所在页（与上述的readPage思路相近）
        //1.先获取要写入磁盘的数据
        byte[] pageData = page.getPageData();
        //2.获取要写入的位置
        try( RandomAccessFile raf = new RandomAccessFile(getFile(), "rw")){
            //根据页号查找位置
            int pos = page.getId().getPageNumber()*BufferPool.getPageSize();
            raf.seek(pos);
            raf.write(pageData);
            //如果是第一次打开，则第一次往文件内添加页不应该增加页数 应为在初始化时，页数已经设置为1  这段代码有无都无所谓。。。。。  我傻了
//            if(!firstOpen){
//                pageNum++;
//            }
//            firstOpen=false;
            //添加新的页进入文件
            pageNum++;
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        pageNum = (int)file.length()/BufferPool.getPageSize();
        return pageNum;
    }

    /**
     * Note that it is important that the HeapFile.insertTuple()
     * and HeapFile.deleteTuple() methods access pages using the BufferPool.getPage() method;
     * otherwise, your implementation of transactions in the next lab will not work properly.
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return
     * @throws DbException
     * @throws IOException
     * @throws TransactionAbortedException
     */
    // see DbFile.java for javadocs
    // 该函数实现是参考别人的代码的
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //返回受到影响的页面
        List<Page> affectedPage = new ArrayList<>();

        //一、对该HeapFile文件内的所有页进行遍历 判断是否存在还有空闲slot的页，调用该页的insert函数，并返回该页
        for(int i=0;i<this.pageNum;i++){
            //获取每页的页id
            HeapPageId heapPageId = new HeapPageId(this.getId(), i);
            //根据页id从BufferPool中取出对应的页面
            HeapPage page = (HeapPage)BufferPool.getPage(tid, heapPageId, Permissions.READ_WRITE);

            //如果当前页面还存在非空的slot 则说明当前页面可以存入tuple
            if(page.getNumEmptySlots()!=0){

                page.insertTuple(t);

                page.markDirty(true, tid);
                affectedPage.add(page);
                break;
            }

        }

        //二、该文件中的所有页都无法写入，创建新的页面写入磁盘，再通过BuffetPool调用该页，再次写入
            //说明文件中不存在符合条件的页面
        if(affectedPage.size()==0){
            //创建新的页面
            //新的页面需要计入当前文件 则其页id的页号就是目前文件中的页数
            HeapPageId heapPageId = new HeapPageId(getId(), pageNum);
            //此处之前漏掉了 一定要加上 保证新页面加入后 文件的总页数随之改变 无意义代码
//            pageNum++;
            //创建空页
            HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
            //写入磁盘
            writePage(heapPage);
            //从BufferPool中读出
            HeapPage page = (HeapPage)BufferPool.getPage(tid, heapPageId, Permissions.READ_WRITE);
            page.insertTuple(t);
            page.markDirty(true, tid);
            affectedPage.add(page);
        }

        return affectedPage;
    }

    /**
     * 按照实验讲义所给的提示，调用页面必须通过BufferPoold的getPage()方法，否则后序实验会出错
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return
     * @throws DbException
     * @throws TransactionAbortedException
     */
    // see DbFile.java for javadocs

    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

        //收到印影响的页
        ArrayList<Page> affetcedPage = new ArrayList<>();

        //获取要删除的元组所在页的id
        PageId pageId = t.getRecordId().getPageId();

        //遍历所有页面进行扫描
        for(int i=0;i<this.pageNum;i++) {
            if(i==pageId.getPageNumber()){
                //根据页id从BufferPool中取出对应的页面
                HeapPage page = (HeapPage) BufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
                page.deleteTuple(t);
                affetcedPage.add(page);

            }
        }

        //根据受到影响的页来判断当前元组是否可以删除，是否存于当前文件及页面中，不在则抛出异常
        if(affetcedPage.size()==0) throw new DbException("不存在要删除的元组，无法完成删除操作");

        return affetcedPage;
        // not necessary for lab1
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     * 返回迭代器，通过BufferPool的getPage函数而非本类中的readPage函数
     */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        //这里试一下在返回表迭代器前，将BufferPool中的所有页面写回磁盘，保证，修改期间
        return new HeapFileIterator(tid);

    }


    /**
     * 参考别人的代码
     */
    private class HeapFileIterator implements  DbFileIterator{

        //file文件内每页的iterator
        private Iterator<Tuple> iterator;
        //当前迭代到的页面
        private int pageNo;
        //事务id
        TransactionId tid;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        private Iterator<Tuple> getIterator(HeapPageId hpid) throws TransactionAbortedException, DbException {
            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
            HeapPage page =(HeapPage) Database.getBufferPool().getPage(tid, hpid, Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNo=0;
            //getId（）返回该HeapFile的唯一标识符 并传入要读取的页
            HeapPageId hpid = new HeapPageId(getId(), pageNo);
            this.iterator = getIterator(hpid);


        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            //迭代器被关闭
            if(iterator==null){
                return false;
            }

            //还有元组 返会true
            if(iterator.hasNext()){
                return true;
            }

            //iterator是整个文件的迭代器 所以当前上述条件都不满足时 需要将该迭代器更新 让他去迭代下一个页面的元组
            //如果当前迭代器迭代到的页面小于总页数
            while(pageNo<numPages()-1) {
                pageNo++;
                HeapPageId hpid = new HeapPageId(getId(), pageNo);
                 iterator = getIterator(hpid);
                 //这里不能直接返回true 因为不知道下一个页面中是否含有有效元组 所以还需再次调用迭代器的hasnext()

                //添加此处代码 并且使if改为上述的while，testAlternateEmptyAndFullPagesThenIterate可以通过
                //应为如果添加在该文件中添加了多个页面，可能出现多个一个页面全是空，一个页面有元组的情况
                //如果只是判断一次不存在元组的页面的下一面不存在元组的情况便返回false，那么可能出现后面多个页面包含元组却无法输出的情况
                if(!iterator.hasNext()&&pageNo<numPages()-1) continue;

                 return iterator.hasNext();
            }

                return false;

        }


        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            //未初始化
            if(iterator==null) throw new NoSuchElementException();

            if(!iterator.hasNext()){
                throw new NoSuchElementException();
            }
            return iterator.next();
        }


        @Override
        public void rewind() throws DbException, TransactionAbortedException {
                //重新初始化一次
            open();

        }

        @Override
        public void close() {
            pageNo=0;
            iterator=null;
        }
    }

}

