package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.index.BTreePage;
import simpledb.myLogger;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.transaction.deadLockDetec;

import java.io.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static  final int DEFAULT_PAGES = 50;

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;
    private static int pageSize = DEFAULT_PAGE_SIZE;

    //页的最大数量
    public  static int PAGES_NUM=DEFAULT_PAGES;
    //缓冲池内的页
   static  ConcurrentHashMap<PageId,Page> pages;
   //锁管理器
   static public LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
//        PAGES_NUM = DEFAULT_PAGES;
//        if(numPages!=DEFAULT_PAGES){
//            try {
//                throw new DbException("超出缓冲池最大页容量");
//            } catch (DbException e) {
//                e.printStackTrace();
//            }
//            PAGES_NUM  = numPages;
//        }

        PAGES_NUM  = numPages;
        pages = new ConcurrentHashMap<>(PAGES_NUM);
        lockManager = new LockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     *   阻塞当前函数直到返回所需页面
     *
     */
    public static Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        //先获取对应页面的锁，再去读入并返回页
        boolean flag = false;
//        if(perm==Permissions.READ_ONLY){
//            flag=lockManager.grantSlock(pid,tid);
//        }else{
//            flag=lockManager.grantXlock(pid,tid);
//        }
        flag = lockManager.grantLock(perm,pid,tid);


        /*这里先采用简单的超出次数抛出异常来实现死锁检测以及回滚操作*/
        deadLockDetec detector = deadLockDetec.single();
        //只要一直没获取对应页面的锁就不断自旋直到获取 但这样开销比较大，可以让线程sleep一段时间
        while(!flag){
      detector.deadLockDetect();
            myLogger.logger.debug("死锁检测");
        /*这里使用环形检测，出现死锁则抛出异常*/
//            if(lockManager.deadlockDetect(tid,pid)){
//                throw new TransactionAbortedException();
//            }
            try {
                myLogger.logger.debug("获取锁失败，休眠一段时间。");
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //继续获取锁
            if(perm==Permissions.READ_ONLY){
                    flag=lockManager.grantSlock(pid,tid);
                }else{
                    flag=lockManager.grantXlock(pid,tid);
            }
//
        }



        //获取了对应页面的锁 可以读入并返回页面

        //查询pageid
        Page page = pages.get(pid);
        //若该页不在缓冲池中
        if(page==null){
            //若缓冲池页数大小到达限定大小 采取驱逐策略 这里先抛出异常
            if(pages.size()>=PAGES_NUM){
                //throw new DbException("缓冲池页数超出限制");
                try {
                    Database.getBufferPool().evictPage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            //从数据库目录中获取指定pid的文件
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            //读出该文件的pid号页 加入到缓冲池中
            page = dbFile.readPage(pid);
            pages.put(pid,page);

        }

        return page;



    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     *          释放特定页面上特定事务的所有锁
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.unlock(tid,pid);
    }


    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdlock(tid,p);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     *
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }



    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     * commit
     *     true--commit
     *        When you commit, you should flush dirty pages associated to the transaction to disk.
     *     false--abort
     *        When you abort, you should revert any changes made by the transaction
     *              by restoring the page to its on-disk state.
     *        无论提交还是终止都需要释放相应事务的所有锁
     */
    public  synchronized void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit){
            //将所有改事务处理的所有脏页写入到磁盘中
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }

        }else{
            //此处应该对终止事务进行过的页面操作进行回滚 但是这部分内容是后续Lab5，6的内容 暂时不写
            //这里先用驱逐操作将脏页驱逐，不写回磁盘。使之前进行的写操作无效，变相达到回滚效果
//            for(PageId pid : pages.keySet()){
//                Page page = pages.get(pid);
//                if(page.isDirty()!=null&&page.isDirty()==(tid)){
//                   pages.set
//                }
//            }

            //应该将脏页回滚成原本在磁盘上的状态 所以要重新读入磁盘中的对应页面再写入缓冲区 起到重新加载的效果
            for(PageId pid : pages.keySet()){
                Page page = pages.get(pid);
                if(page.isDirty()!=null&&page.isDirty().equals(tid)){
                    DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                    //获取到了要被修改前的页面
                    Page beforeModifyPage = databaseFile.readPage(page.getId());
                    //将该页面覆盖掉被中断事物处理的页面
                    pages.put(page.getId(),beforeModifyPage);

                }
            }

        }

        //释放所有的锁
        lockManager.unTransactionIdlock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //根据tableId获取要插入元组的表
        DbFile  dbFile = Database.getCatalog().getDatabaseFile(tableId);
        //调用该表的插入元组函数
        List<Page> affetced = dbFile.insertTuple(tid, t);

        //获取收到影响的页面，按照要求将这些页面进行makedirt
        for(Page page : affetced){
            page.markDirty(true, tid);
            //把修改过的页面（在缓冲池内的）重新写入disk
           // HeapFile databaseFile =(HeapFile) Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            //databaseFile.writePage(page);
        }


    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //获取要删除的表
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        //调用表进行删除操作并获取搜到影响的页面
        ArrayList<Page> affetced = (ArrayList<Page>) file.deleteTuple(tid, t);
        //按照要求将受到影响的页面makedirt
        for (Page page : affetced){
            page.markDirty(true, tid);
            //把修改过的页面重新写入disk
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            //databaseFile.writePage(page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     *     将BufferPool中的所有脏页页面刷新到disk中，但不驱逐任何页面
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        //遍历所有的在BufferPool中的表，查看他们是否为脏页。
        // 是就调用其所在表的write函数将该页写回disk
        for(PageId pageId : pages.keySet()){
           // Page page = pages.get(pageId);
//            if(page.isDirty()!=null){
//                DbFile databaseFile = null;
//                //实验5时进行扩展，包括了BTreePage和HeapPage
//                if(page instanceof HeapPage){
//                     databaseFile = Database.getCatalog().getDatabaseFile(((HeapPage)page).pid.getTableId());
//
//                }else if(page instanceof BTreePage){
//                    databaseFile = Database.getCatalog().getDatabaseFile(((BTreePage)page).getId().getTableId());
//
//                }
//                databaseFile.writePage(page);
//            }
            flushPage(pageId);
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     *
     *    将特定页面刷新到disk，不驱逐
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = pages.get(pid);
        if(page!=null){
            DbFile databaseFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
            // append an update record to the log, with
            // a before-image and after-image.
            TransactionId dirtier = page.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                Database.getLogFile().force();
            }
            databaseFile.writePage(page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     * 将特定事务处理过的脏页刷新到disk内
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //获取所有页面进行递归判断是否为tid的脏页
            for(PageId pid : pages.keySet()){
                Page page = pages.get(pid);
                if(page.isDirty()!=null&&page.isDirty()==tid){
                    flushPage(page.getId());
                    // use current page contents as the before-image
                    // for the next transaction that modifies this page.
                    page.setBeforeImage();
                }
            }

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * 驱逐策略---FIFO，将最早使用的页进行驱逐
     * 驱逐到脏页时应该将页面写入磁盘（调用flushpage函数），避免一致性问题
     *
     * LAB4 不应该驱逐任何脏页，页面的重新写入应该在事务提交之后。
     * 所以驱逐页面时，需要驱逐一个clean页面，该clean页面可能以及被某个事务锁定，需要在lockmanager中解锁
     * 如果全是dirty页面就抛出异常。
     *
     */
    private synchronized   void evictPage() throws DbException, IOException {
        // some code goes here
        // not necessary for lab1
        if(pages.size()<1) throw  new DbException("BufferPool中不存在页面，无法进行驱逐");

        for(PageId pageId : pages.keySet()){
            Page page = pages.get(pageId);
            //页面存在
            if(page!=null){
                //页面是脏页 写入磁盘
                //if(page.isDirty()!=null) flushPage(page.getId());
                //是脏页继续寻找clean的页，要记得释放该页上的锁
                if(page.isDirty()!=null) continue;

                //释放给这个页加上的所有锁
                lockManager.unPageLock(pageId);
                //找到clean页就驱逐
                pages.remove(pageId);
                //退出
                return ;
            }
        }

        //走到这里说明缓冲区内的页全部都是脏页 直接抛出异常
        throw new DbException("全为脏页，无法驱逐");
    }

}
