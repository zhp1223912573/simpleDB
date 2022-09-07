
package simpledb.storage;

import simpledb.common.Database;
import simpledb.transaction.TransactionId;
import simpledb.common.Debug;
import sun.security.krb5.internal.tools.Kinit;

import javax.swing.text.Keymap;
import java.io.*;
import java.util.*;
import java.lang.reflect.*;

/*
LogFile implements the recovery subsystem of SimpleDb.  This class is
able to write different log records as needed, but it is the
responsibility of the caller to ensure that write ahead logging and
two-phase locking discipline are followed.  <p>

<u> Locking note: </u>
<p>

Many of the methods here are synchronized (to prevent concurrent log
writes from happening); many of the methods in BufferPool are also
synchronized (for similar reasons.)  Problem is that BufferPool writes
log records (on page flushed) and the log file flushes BufferPool
pages (on checkpoints and recovery.)  This can lead to deadlock.  For
that reason, any LogFile operation that needs to access the BufferPool
must not be declared synchronized and must begin with a block like:

<p>
<pre>
    synchronized (Database.getBufferPool()) {
       synchronized (this) {

       ..

       }
    }
</pre>
*/

/**
<p> The format of the log file is as follows:

<ul>

<li> The first long integer of the file represents the offset of the
last written checkpoint, or -1 if there are no checkpoints

<li> All additional data in the log consists of log records.  Log
records are variable length.

<li> Each log record begins with an integer type and a long integer
transaction id.

 the position in the log file where the record began.
 <li> Each log record ends with a long integer file offset representing

<li> There are five record types: ABORT, COMMIT, UPDATE, BEGIN, and
CHECKPOINT

<li> ABORT, COMMIT, and BEGIN records contain no additional data

<li>UPDATE RECORDS consist of two entries, a before image and an
after image.  These images are serialized Page objects, and can be
accessed with the LogFile.readPageData() and LogFile.writePageData()
methods.  See LogFile.print() for an example.

<li> CHECKPOINT records consist of active transactions at the time
the checkpoint was taken and their first log record on disk.  The format
of the record is an integer count of the number of transactions, as well
as a long integer transaction id and a long integer first record offset
for each active transaction.

</ul>
*/
public class LogFile {

    final File logFile;
    private RandomAccessFile raf;
    Boolean recoveryUndecided; // no call to recover() and no append to log

    static final int ABORT_RECORD = 1;
    static final int COMMIT_RECORD = 2;
    static final int UPDATE_RECORD = 3;
    static final int BEGIN_RECORD = 4;
    static final int CHECKPOINT_RECORD = 5;
    static final long NO_CHECKPOINT_ID = -1;

    final static int INT_SIZE = 4;
    final static int LONG_SIZE = 8;

    long currentOffset = -1;//protected by this
//    int pageSize;
    int totalRecords = 0; // for PatchTest //protected by this

    final Map<Long,Long> tidToFirstLogRecord = new HashMap<>();

    /** Constructor.
        Initialize and back the log file with the specified file.
        We're not sure yet whether the caller is creating a brand new DB,
        in which case we should ignore the log file, or whether the caller
        will eventually want to recover (after populating the Catalog).
        So we make this decision lazily: if someone calls recover(), then
        do it, while if someone starts adding log file entries, then first
        throw out the initial log file contents.

        @param f The log file's name
    */
    public LogFile(File f) throws IOException {
	this.logFile = f;
        raf = new RandomAccessFile(f, "rw");
        recoveryUndecided = true;

        // install shutdown hook to force cleanup on close
        // Runtime.getRuntime().addShutdownHook(new Thread() {
                // public void run() { shutdown(); }
            // });

        //XXX WARNING -- there is nothing that verifies that the specified
        // log file actually corresponds to the current catalog.
        // This could cause problems since we log tableids, which may or
        // may not match tableids in the current catalog.
    }

    // we're about to append a log record. if we weren't sure whether the
    // DB wants to do recovery, we're sure now -- it didn't. So truncate
    // the log.
    void preAppend() throws IOException {
        totalRecords++;
        if(recoveryUndecided){
            recoveryUndecided = false;
            raf.seek(0);
            raf.setLength(0);
            raf.writeLong(NO_CHECKPOINT_ID);
            raf.seek(raf.length());
            currentOffset = raf.getFilePointer();
        }
    }

    public synchronized int getTotalRecords() {
        return totalRecords;
    }
    
    /** Write an abort record to the log for the specified tid, force
        the log to disk, and perform a rollback
        @param tid The aborting transaction.
    */
    public void logAbort(TransactionId tid) throws IOException {
        // must have buffer pool lock before proceeding, since this
        // calls rollback

        synchronized (Database.getBufferPool()) {

            synchronized(this) {
                preAppend();
                //Debug.log("ABORT");
                //should we verify that this is a live transaction?

                // must do this here, since rollback only works for
                // live transactions (needs tidToFirstLogRecord)
                rollback(tid);

                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid.getId());
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                force();
                tidToFirstLogRecord.remove(tid.getId());
            }
        }
    }

    /** Write a commit record to disk for the specified tid,
        and force the log to disk.

        @param tid The committing transaction.
    */
    public synchronized void logCommit(TransactionId tid) throws IOException {
        preAppend();
        Debug.log("COMMIT " + tid.getId());
        //should we verify that this is a live transaction?

        raf.writeInt(COMMIT_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();
        force();
        tidToFirstLogRecord.remove(tid.getId());
    }

    /** Write an UPDATE record to disk for the specified tid and page
        (with provided         before and after images.)
        @param tid The transaction performing the write
        @param before The before image of the page
        @param after The after image of the page

        @see Page#getBeforeImage
    */
    public  synchronized void logWrite(TransactionId tid, Page before,
                                       Page after)
        throws IOException  {
        Debug.log("WRITE, offset = " + raf.getFilePointer());
        preAppend();
        /* update record conists of

           record type
           transaction id
           before page data (see writePageData)
           after page data
           start offset
        */
        raf.writeInt(UPDATE_RECORD);
        raf.writeLong(tid.getId());

        writePageData(raf,before);
        writePageData(raf,after);
        raf.writeLong(currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("WRITE OFFSET = " + currentOffset);
    }

    void writePageData(RandomAccessFile raf, Page p) throws IOException{
        PageId pid = p.getId();
        int[] pageInfo = pid.serialize();

        //page data is:
        // page class name
        // id class name
        // id class bytes
        // id class data
        // page class bytes
        // page class data

        String pageClassName = p.getClass().getName();
        String idClassName = pid.getClass().getName();

        raf.writeUTF(pageClassName);
        raf.writeUTF(idClassName);

        raf.writeInt(pageInfo.length);
        for (int j : pageInfo) {
            raf.writeInt(j);
        }
        byte[] pageData = p.getPageData();
        raf.writeInt(pageData.length);
        raf.write(pageData);
        //        Debug.log ("WROTE PAGE DATA, CLASS = " + pageClassName + ", table = " +  pid.getTableId() + ", page = " + pid.pageno());
    }

    Page readPageData(RandomAccessFile raf) throws IOException {
        PageId pid;
        Page newPage = null;

        String pageClassName = raf.readUTF();
        String idClassName = raf.readUTF();

        try {
            Class<?> idClass = Class.forName(idClassName);
            Class<?> pageClass = Class.forName(pageClassName);

            Constructor<?>[] idConsts = idClass.getDeclaredConstructors();
            int numIdArgs = raf.readInt();
            Object[] idArgs = new Object[numIdArgs];
            for (int i = 0; i<numIdArgs;i++) {
                idArgs[i] = raf.readInt();
            }
            pid = (PageId)idConsts[0].newInstance(idArgs);

            Constructor<?>[] pageConsts = pageClass.getDeclaredConstructors();
            int pageSize = raf.readInt();

            byte[] pageData = new byte[pageSize];
            raf.read(pageData); //read before image

            Object[] pageArgs = new Object[2];
            pageArgs[0] = pid;
            pageArgs[1] = pageData;

            newPage = (Page)pageConsts[0].newInstance(pageArgs);

            //            Debug.log("READ PAGE OF TYPE " + pageClassName + ", table = " + newPage.getId().getTableId() + ", page = " + newPage.getId().pageno());
        } catch (ClassNotFoundException | InvocationTargetException | IllegalAccessException | InstantiationException e){
            e.printStackTrace();
            throw new IOException();
        }
        return newPage;

    }

    /** Write a BEGIN record for the specified transaction
        @param tid The transaction that is beginning

    */
    public synchronized  void logXactionBegin(TransactionId tid)
        throws IOException {
        Debug.log("BEGIN");
        if(tidToFirstLogRecord.get(tid.getId()) != null){
            System.err.print("logXactionBegin: already began this tid\n");
            throw new IOException("double logXactionBegin()");
        }
        preAppend();
        raf.writeInt(BEGIN_RECORD);
        raf.writeLong(tid.getId());
        raf.writeLong(currentOffset);
        tidToFirstLogRecord.put(tid.getId(), currentOffset);
        currentOffset = raf.getFilePointer();

        Debug.log("BEGIN OFFSET = " + currentOffset);
    }

    /** Checkpoint the log and write a checkpoint record. */
    public void logCheckpoint() throws IOException {
        //make sure we have buffer pool lock before proceeding
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                //Debug.log("CHECKPOINT, offset = " + raf.getFilePointer());
                preAppend();
                long startCpOffset, endCpOffset;
                Set<Long> keys = tidToFirstLogRecord.keySet();
                Iterator<Long> els = keys.iterator();
                force();
                Database.getBufferPool().flushAllPages();
                startCpOffset = raf.getFilePointer();
                raf.writeInt(CHECKPOINT_RECORD);
                raf.writeLong(-1); //no tid , but leave space for convenience

                //write list of outstanding transactions
                raf.writeInt(keys.size());
                while (els.hasNext()) {
                    Long key = els.next();
                    Debug.log("WRITING CHECKPOINT TRANSACTION ID: " + key);
                    raf.writeLong(key);
                    //Debug.log("WRITING CHECKPOINT TRANSACTION OFFSET: " + tidToFirstLogRecord.get(key));
                    raf.writeLong(tidToFirstLogRecord.get(key));
                }

                //once the CP is written, make sure the CP location at the
                // beginning of the log file is updated
                endCpOffset = raf.getFilePointer();
                raf.seek(0);
                raf.writeLong(startCpOffset);
                raf.seek(endCpOffset);
                raf.writeLong(currentOffset);
                currentOffset = raf.getFilePointer();
                //Debug.log("CP OFFSET = " + currentOffset);
            }
        }

        logTruncate();
    }

    /** Truncate any unneeded portion of the log to reduce its space
        consumption */
    public synchronized void logTruncate() throws IOException {
        preAppend();
        raf.seek(0);
        long cpLoc = raf.readLong();

        long minLogRecord = cpLoc;

        if (cpLoc != -1L) {
            raf.seek(cpLoc);
            int cpType = raf.readInt();
            @SuppressWarnings("unused")
            long cpTid = raf.readLong();

            if (cpType != CHECKPOINT_RECORD) {
                throw new RuntimeException("Checkpoint pointer does not point to checkpoint record");
            }

            int numOutstanding = raf.readInt();

            for (int i = 0; i < numOutstanding; i++) {
                @SuppressWarnings("unused")
                long tid = raf.readLong();
                long firstLogRecord = raf.readLong();
                if (firstLogRecord < minLogRecord) {
                    minLogRecord = firstLogRecord;
                }
            }
        }

        // we can truncate everything before minLogRecord
        File newFile = new File("logtmp" + System.currentTimeMillis());
        RandomAccessFile logNew = new RandomAccessFile(newFile, "rw");
        logNew.seek(0);
        logNew.writeLong((cpLoc - minLogRecord) + LONG_SIZE);

        raf.seek(minLogRecord);

        //have to rewrite log records since offsets are different after truncation
        while (true) {
            try {
                int type = raf.readInt();
                long record_tid = raf.readLong();
                long newStart = logNew.getFilePointer();

                Debug.log("NEW START = " + newStart);

                logNew.writeInt(type);
                logNew.writeLong(record_tid);

                switch (type) {
                case UPDATE_RECORD:
                    Page before = readPageData(raf);
                    Page after = readPageData(raf);

                    writePageData(logNew, before);
                    writePageData(logNew, after);
                    break;
                case CHECKPOINT_RECORD:
                    int numXactions = raf.readInt();
                    logNew.writeInt(numXactions);
                    while (numXactions-- > 0) {
                        long xid = raf.readLong();
                        long xoffset = raf.readLong();
                        logNew.writeLong(xid);
                        logNew.writeLong((xoffset - minLogRecord) + LONG_SIZE);
                    }
                    break;
                case BEGIN_RECORD:
                    tidToFirstLogRecord.put(record_tid,newStart);
                    break;
                }

                //all xactions finish with a pointer
                logNew.writeLong(newStart);
                raf.readLong();

            } catch (EOFException e) {
                break;
            }
        }

        Debug.log("TRUNCATING LOG;  WAS " + raf.length() + " BYTES ; NEW START : " + minLogRecord + " NEW LENGTH: " + (raf.length() - minLogRecord));

        raf.close();
        logFile.delete();
        newFile.renameTo(logFile);
        raf = new RandomAccessFile(logFile, "rw");
        raf.seek(raf.length());
        newFile.delete();

        currentOffset = raf.getFilePointer();
        //print();
    }

    /** Rollback the specified transaction, setting the state of any
        of pages it updated to their pre-updated state.  To preserve
        transaction semantics, this should not be called on
        transactions that have already committed (though this may not
        be enforced by this method.)

        @param tid The transaction to rollback
    */
    public void rollback(TransactionId tid)
        throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                //仿照logTruncate函数
                /*
                本应该从tid第一次出现的位置读取所有tid事务的更新记录，进行逆序重做的操作
                  但是update操作时是对整个页面进行更新，所有这里采用简化的方式，直接读取
                  事务tid第一次更新的页面操作，将数据直接恢复倒最初状态，不去一步一步回滚。
                  但是回滚之后，需要直接退出。
                */
                //1.找到事务tid所在得到位置
                //2.开始读取文件，对不同记录分别处理
                //4读到updateRecord，正式回滚文件，在抛弃缓冲中的该页，并且删除tidTofirstRecord记录
                //

                //1.
                Long offset = tidToFirstLogRecord.get(tid.getId());
                if(offset!=null)
                    raf.seek(offset);
                boolean isRollBack = false;

                while (true) {
                    try {
                        //2.
                        int type = raf.readInt();
                        long record_tid = raf.readLong();
                        //3.
                        switch (type) {
                            case UPDATE_RECORD:
                                Page before = readPageData(raf);
                                Page after = readPageData(raf);
                                if(tid.getId()==record_tid){
                                    Database.getCatalog().getDatabaseFile(before.getId().getTableId()).writePage(before);
                                    Database.getBufferPool().discardPage(before.getId());
                                    tidToFirstLogRecord.remove(tid);
                                    isRollBack = true;
                                }
                                break;
                            case CHECKPOINT_RECORD:
                                int numXactions = raf.readInt();
                                while (numXactions-- > 0) {
                                    long xid = raf.readLong();
                                    long xoffset = raf.readLong();
                                }
                                break;
                        }

                        raf.readLong();

                        if(isRollBack){
                            break;
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }

            }
        }
    }

    public void rollback(Long tid)
            throws NoSuchElementException, IOException {
        synchronized (Database.getBufferPool()) {
            synchronized(this) {
                preAppend();
                // some code goes here
                //仿照logTruncate函数
                /*
                本应该从tid第一次出现的位置读取所有tid事务的更新记录，进行逆序重做的操作
                  但是update操作时是对整个页面进行更新，所有这里采用简化的方式，直接读取
                  事务tid第一次更新的页面操作，将数据直接恢复倒最初状态，不去一步一步回滚。
                  但是回滚之后，需要直接退出。
                */
                //1.找到事务tid所在得到位置
                //2.开始读取文件，对不同记录分别处理
                //4读到updateRecord，正式回滚文件，在抛弃缓冲中的该页，并且删除tidTofirstRecord记录
                //

                //1.
                Long offset = tidToFirstLogRecord.get(tid);
                if(offset!=null)
                    raf.seek(offset);
                boolean isRollBack = false;

                while (true) {
                    try {
                        //2.
                        int type = raf.readInt();
                        long record_tid = raf.readLong();
                        //3.
                        switch (type) {
                            case UPDATE_RECORD:
                                Page before = readPageData(raf);
                                Page after = readPageData(raf);
                                if(tid==record_tid){
                                    Database.getCatalog().getDatabaseFile(before.getId().getTableId()).writePage(before);
                                    Database.getBufferPool().discardPage(before.getId());
                                    tidToFirstLogRecord.remove(tid);
                                    isRollBack = true;
                                }
                                break;
                            case CHECKPOINT_RECORD:
                                int numXactions = raf.readInt();
                                while (numXactions-- > 0) {
                                    long xid = raf.readLong();
                                    long xoffset = raf.readLong();
                                }
                                break;
                        }

                        raf.readLong();

                        if(isRollBack){
                            break;
                        }
                    } catch (EOFException e) {
                        raf.seek(currentOffset);
                        break;
                    }
                }

            }
        }
    }

    /** Shutdown the logging system, writing out whatever state
        is necessary so that start up can happen quickly (without
        extensive recovery.)
    */
    public synchronized void shutdown() {
        try {
            logCheckpoint();  //simple way to shutdown is to write a checkpoint record
            raf.close();
        } catch (IOException e) {
            System.out.println("ERROR SHUTTING DOWN -- IGNORING.");
            e.printStackTrace();
        }
    }

    /** Recover the database system by ensuring that the updates of
        committed transactions are installed and that the
        updates of uncommitted transactions are not installed
     算法实现参考：操作系统概论第六版-16.4.2
    */
    public void recover() throws IOException {
        synchronized (Database.getBufferPool()) {
            synchronized (this) {
                recoveryUndecided = false;
                // some code goes here
                //1.找到最后一个检查点，获取该检查点时的活跃事务id，并用集合L保存
                //2.重做阶段：开始从保存点向下查找每条记录，对每条更新记录进行redo操作。
                //          若出现新的事务，加入集合。若出现事务的abort和commit记录，将该事务移出L
                //3.扫描完成，进入撤销阶段
                //4.撤销阶段：总日志末尾反向移动，每碰到日志中的一个事务update记录，就执行undo操作
                //          并加入一条abort记录，并从L中移除该事务
                //5.不断重复，直到集合L为空

                // The first long integer of the file represents the offset of the
                //last written checkpoint, or -1 if there are no checkpoints
                //1.
                print();
                raf.seek(0);
                long lastCheckPoint = raf.readLong();
                //所有需要undo操作的页面
                Map<Long, Long> tid2offset = new HashMap<>();
                //某一事务操作的页面集合
                Map<Long, List<PageId>> tid2pages = new HashMap<>();
                //页面id对应的页面
                Map<PageId, Page> pages = new HashMap<>();

                //获取活跃的事务
                if (lastCheckPoint != -1) {
                    //定位倒checkpoint所在位置
                    raf.seek(lastCheckPoint);
                    //获取checkpoint记录信息
                    int type = raf.readInt();//类型
                    if (type == CHECKPOINT_RECORD) {
                        raf.readLong();//事务id
                        int nums = raf.readInt();
                        for (int i = 0; i < nums; i++) {
                            Long tid = raf.readLong();
                            Long offset = raf.readLong();
                            tid2offset.put(tid, offset);
                        }
                    }
                    raf.readLong();
                }
                //为已存在的活跃事务初始化一个页集合
                Set<Long> longs = tid2offset.keySet();
                for (Long tid : longs) {
                    //需要该集合在rollback函数与rocver进行建立连接，在重做阶段找到要撤销的事务，将其与偏移量加在一起，为rollback使用。
                    tidToFirstLogRecord.put(tid,tid2offset.get(tid));
                    tid2pages.put(tid, new ArrayList<>());
                }



                //2.
                while (true) {
                    try {
                        int type = raf.readInt();
                        long record_tid = raf.readLong();

                        switch (type) {
                            //更新就保存页面，直到遇见commit和abort再进行处理
                            case UPDATE_RECORD:
                                Page before = readPageData(raf);
                                Page after = readPageData(raf);
                                raf.readLong();
                                //只保存after页面 等到遇到commit时再正式更新
                                tid2pages.get(record_tid).add(after.getId());
                                pages.put(after.getId(), after);
                                break;

                            case BEGIN_RECORD:
                                //出现begin，将该事务加入
                                long offset = raf.readLong();
                                tidToFirstLogRecord.put(record_tid,offset);
                                tid2offset.put(record_tid,offset);
                                tid2pages.put(record_tid,new ArrayList<>());
                                break;

                            case ABORT_RECORD:
                                //出现终止 剔除tid2offset中的对应事务
                                tidToFirstLogRecord.remove(record_tid);
                                ArrayList<PageId> pageArrayList = (ArrayList<PageId>) tid2pages.get(record_tid);
                                tid2pages.remove(record_tid);
                                for (PageId pageId : pageArrayList) {
                                    pages.remove(pageId);
                                }
                                raf.readLong();
                                break;

                            case COMMIT_RECORD:
                                //将tid2offset中的事务提交
                                ArrayList<PageId> pagelist = (ArrayList<PageId>) tid2pages.get(record_tid);
                                for (PageId pageId : pagelist) {
                                    Page page = pages.get(pageId);
                                    Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
                                    page.setBeforeImage();
                                }
                                tidToFirstLogRecord.remove(record_tid);
                                tid2pages.remove(record_tid);
                                raf.readLong();
                                break;
                        }
                    } catch (EOFException e) {
                        break;
                    }
                }



            //4.撤销阶段

            currentOffset = raf.getFilePointer();
            //获取需要进行撤销操作的事务的记录位置，方便重做后写入abort信息
            for (Long tid : tid2offset.keySet()) {
                raf.seek(currentOffset);
                raf.writeInt(ABORT_RECORD);
                raf.writeLong(tid);
                raf.writeLong(currentOffset);
                //开始回滚
                force();
                rollback(tid);
                //将abort记录设置在文件末尾
                currentOffset = raf.length(); }
            }
            raf.writeLong(currentOffset);
            print();
        }
    }


    /** Print out a human readable represenation of the log */
    public void print() throws IOException {
        long curOffset = raf.getFilePointer();

        raf.seek(0);

        System.out.println("0: checkpoint record at offset " + raf.readLong());

        while (true) {
            try {
                int cpType = raf.readInt();
                long cpTid = raf.readLong();

                System.out.println((raf.getFilePointer() - (INT_SIZE + LONG_SIZE)) + ": RECORD TYPE " + cpType);
                System.out.println((raf.getFilePointer() - LONG_SIZE) + ": TID " + cpTid);

                switch (cpType) {
                case BEGIN_RECORD:
                    System.out.println(" (BEGIN)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case ABORT_RECORD:
                    System.out.println(" (ABORT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;
                case COMMIT_RECORD:
                    System.out.println(" (COMMIT)");
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());
                    break;

                case CHECKPOINT_RECORD:
                    System.out.println(" (CHECKPOINT)");
                    int numTransactions = raf.readInt();
                    System.out.println((raf.getFilePointer() - INT_SIZE) + ": NUMBER OF OUTSTANDING RECORDS: " + numTransactions);

                    while (numTransactions-- > 0) {
                        long tid = raf.readLong();
                        long firstRecord = raf.readLong();
                        System.out.println((raf.getFilePointer() - (LONG_SIZE + LONG_SIZE)) + ": TID: " + tid);
                        System.out.println((raf.getFilePointer() - LONG_SIZE) + ": FIRST LOG RECORD: " + firstRecord);
                    }
                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                case UPDATE_RECORD:
                    System.out.println(" (UPDATE)");

                    long start = raf.getFilePointer();
                    Page before = readPageData(raf);

                    long middle = raf.getFilePointer();
                    Page after = readPageData(raf);

                    System.out.println(start + ": before image table id " + before.getId().getTableId());
                    System.out.println((start + INT_SIZE) + ": before image page number " + before.getId().getPageNumber());
                    System.out.println((start + INT_SIZE) + " TO " + (middle - INT_SIZE) + ": page data");

                    System.out.println(middle + ": after image table id " + after.getId().getTableId());
                    System.out.println((middle + INT_SIZE) + ": after image page number " + after.getId().getPageNumber());
                    System.out.println((middle + INT_SIZE) + " TO " + (raf.getFilePointer()) + ": page data");

                    System.out.println(raf.getFilePointer() + ": RECORD START OFFSET: " + raf.readLong());

                    break;
                }

            } catch (EOFException e) {
                //e.printStackTrace();
                break;
            }
        }

        // Return the file pointer to its original position
        raf.seek(curOffset);
    }

    public  synchronized void  force() throws IOException {
        raf.getChannel().force(true);
    }

}
