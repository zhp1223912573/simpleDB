package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * @author zhp
 * @date 2022-03-20 15:00
 * 锁管理器--对页和事务进行管理
 * 对每个页保存持有该页的锁的事务以及锁类型的信息，因为可能有多个事务持有该页所以使用一个list结构保存
 *      --这里用一个锁状态来保持LockStat来保存
 * 对每个等待持有某个已经被加上排他锁的页的事务进行保存
 *
 *
 */
public class LockManager {

    //锁表 被加了锁的页表
    private Map<PageId, List<LockStat>> lockTable;
    //等待锁释放的事务
    private Map<TransactionId,PageId> waitTable;

    public LockManager() {
        //使用ConcurrentHashMap避免ConcurrentModificationException
        this.lockTable = new ConcurrentHashMap<>();
        this.waitTable = new ConcurrentHashMap<>();
    }


    public boolean grantLock(Permissions perm,PageId pid,TransactionId tid){
        if(perm==Permissions.READ_ONLY){
           return grantSlock(pid,tid);
        }else{
            return grantXlock(pid,tid);
        }
    }
    /**
     * 获取页面读锁
     * Before a transaction can read an object, it must have a shared lock on it.
     * Multiple transactions can have a shared lock on an object.
     * @param pid
     * @param tid
     * @Param permissions
     * @return
     */
    public synchronized boolean grantSlock(PageId pid, TransactionId tid){
        //1.在锁表中查找页面存在与否
        List<LockStat> lockStats = lockTable.get(pid);
        //2.存在则读取其锁信息再进行判断
        if(lockStats!=null&&lockStats.size()!=0){
            //2.1锁信息列表中只有一个锁
            if(lockStats.size()==1){
                LockStat lockStat = lockStats.get(0);
                //判断唯一的锁是否属于自己是就返回true，
               if(lockStat.getTransactionId()==tid){
                   //属于自己并且是读锁就直接返回true
                   if(lockStat.getPermissions()==Permissions.READ_ONLY){
                       return true;
                   }else{
                       //属于自己但是是排他锁，那就将读锁加入
                      return lock(Permissions.READ_ONLY, tid, pid);
                   }
               }else{//不是就判断是读锁还是排他锁
                   if(lockStat.getPermissions()==Permissions.READ_ONLY){
                       return lock(Permissions.READ_ONLY, tid, pid);
                   }else{
                       //是其他事务的排他锁就加入等待表
                       return wait(tid,pid);
                   }
               }
            }
            //2.2锁信息列表有多个锁，读取每个锁信息
            else{
                //是否包含排他锁
                for(LockStat lockStat : lockStats){
                    //包含 判断是不是当前事务的锁 是就直接true 不是将当前事务加入等待
                    if(lockStat.getPermissions()==Permissions.READ_WRITE){
                        if(lockStat.getTransactionId()==tid){
                            return true;
                        }else{
                            return wait(tid,pid);
                        }
                    }else{
                        //不包含   出现当前事务的读锁 直接返回true 没出现当前事务的锁 将当前事务加入
                        if(lockStat.getTransactionId()==tid){
                            return true;
                        }
                    }
                }
                //读完所有的锁都没返回 说明既不存在排他锁，也不存在当前事务的读锁
                return lock(Permissions.READ_ONLY, tid, pid);
            }

        }
        //3.不存在则直接将目前页面加入锁表
        else{
            return lock(Permissions.READ_ONLY, tid, pid);
        }

    }

    /**获取页面写锁
     * Before a transaction can write an object, it must have an exclusive lock on it.
     * Only one transaction may have an exclusive lock on an object.
     * If transaction t is the only transaction holding a shared lock on an object o,
     * t may upgrade its lock on o to an exclusive lock.
     * @param pid
     * @param tid
     * @return
     */
    public synchronized boolean grantXlock(PageId pid,TransactionId tid){
        //获取锁表中目标页信息
        List<LockStat> lockStats = lockTable.get(pid);

        //目标页信息为空或者为0(因为先前可能存在锁被删除了，所以锁状态不为空但为0)直接加排他锁
        if(lockStats==null||lockStats.size()==0){
           return lock(Permissions.READ_WRITE, tid, pid);
        }

        //目标页信息非空且不为0，则进行判断
       //只有一个锁
        if(lockStats.size()==1){
            LockStat lockStat = lockStats.get(0);
            if(lockStat.getTransactionId()==tid){
                if(lockStat.getPermissions()==Permissions.READ_WRITE){
                    //是自己的排他锁直接返回
                    return true;
                }else{
                    //是自己的读锁就加排他锁
                    return  lock(Permissions.READ_WRITE, tid, pid);
                }
            }
        }

        //两个锁
        /*在测试时，发现申请写锁时，可能存在申请资源的事务在该资源上有同个事务的多个读锁
         这种情况下对读锁的次数进行统计，若读锁出现的次数和等同于锁状态中的所有锁总数，
         表示可以加写锁
         */
        if(lockStats.size()>1){
            int count =0;
            for(LockStat lockStat : lockStats){
                //包含自己的排他锁直接返回true
                if(lockStat.getTransactionId()==tid){
                    if(lockStat.getPermissions()==Permissions.READ_WRITE){
                         return true;
                    }else{//此处代码是在实验5的练习1中添加的
                        count++;
                    }
                 }
            }
            if(count==lockStats.size()){
                //符合条件 加写锁
                return  lock(Permissions.READ_WRITE, tid, pid);
            }
        }

        //剩下的所有情况都应该使当前事务进入等待队列
        return wait(tid,pid);

    }

    /**
     *  lock操作
     * @param permissions
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean lock(Permissions permissions,TransactionId tid,PageId pid){
        //获取页表的锁状态信息
        List<LockStat> lockStats = lockTable.get(pid);
        //判断是否为空
        if (lockStats == null) {
            lockStats = new ArrayList<>();
        }
        //添加当前锁状态
        lockStats.add(new LockStat(tid, permissions));
        //补充到锁表中
        lockTable.put(pid, lockStats);
        //移除等待表中的同一等待事务
        waitTable.remove(tid);
        return true;
    }

    /**
     * wait操作 将等待页面加入等待页表
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean wait(TransactionId tid,PageId pid){
        waitTable.put(tid,pid);
        return false;
    }

    /**
     * 释放锁操作
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean unlock(TransactionId tid,PageId pid){
        //获取目标页的锁信息
        List<LockStat> lockStats =(ArrayList<LockStat>) lockTable.get(pid);
        //为空或不存在锁 无锁可以释放 直接返回false
        if(lockStats==null||lockStats.size()==0) return false;

        //若不为空说明当前页表中存在某些事务的锁信息 调用函数寻找
        LockStat lockStat = getLockStat(tid, pid);
        //未找到了要删除的锁信息
        if(lockStat==null){
           return false;
        }
        //找到要删除的锁
        lockStats.remove(lockStat);
        //重新将锁信息写入
        lockTable.put(pid, lockStats);
        return true;
    }

    /**
     * 释放特定事务的所有锁
     * @param tid
     * @return
     */
    public synchronized boolean unTransactionIdlock(TransactionId tid){
        ArrayList<PageId> pids = getAllTranscationStat(tid);
        for(PageId pageId : pids){
            unlock(tid, pageId);
        }
        return true;
    }

    /**
     * 释放页上的所有锁
     */
    public synchronized boolean unPageLock(PageId pageId){
        if(lockTable.get(pageId)!=null){
            lockTable.remove(pageId);
            return true;
        }
        return false;
    }

    /**
     * 获取锁表中特定事务的所有锁信息
     * @param tid
     * @return
     */
    private synchronized ArrayList<PageId> getAllTranscationStat(TransactionId tid) {
        ArrayList<PageId> pids  = new ArrayList<>();
        for (Map.Entry<PageId, List<LockStat>> entry : lockTable.entrySet()) {
            for (LockStat ls : entry.getValue()) {
                if (ls.getTransactionId().equals(tid)) {
                    pids.add(entry.getKey());
                }
            }
        }
        return pids;
    }

    /**
     * 获取具体页中释放存在具体事务的锁状态
     * @param tid
     * @param pid
     * @return
     */
    public synchronized LockStat getLockStat(TransactionId tid,PageId pid){
        List<LockStat> lockStats = lockTable.get(pid);
        for(LockStat lockStat : lockStats){
            if(lockStat.getTransactionId().equals(tid)){
                return lockStat;
            }
        }
        return null;
    }

    /**
     * 判断页面上是否包含特定事务的锁
     * @param tid
     * @param pid
     * @return
     */
    public synchronized boolean holdlock(TransactionId tid,PageId pid){
        List<LockStat> lockStats = lockTable.get(pid);
        if(lockStats==null||lockStats.size()==0) return false;
        for(LockStat lockStat : lockStats){
            if(lockStat.getTransactionId()==tid){
                return true;
            }
        }
        return false;
    }

    /**
     * 死锁检测:环形检测
     * @param tid
     * @param pid
     * @return
     * 死锁发生的原因是：
     * 申请事务申请的资源的持有者正在直接或间接地等待申请者所持有的资
     *
    * */
    public synchronized boolean deadlockDetect(TransactionId tid,PageId pid){
        //申请者所需资源的持有者
        List<LockStat> holders = lockTable.get(pid);
        //持有者不存在或者为0 说明申请者可以直接申请 不存在死锁情况
        if(holders==null||holders.size()==0){
            return false;
        }

        //获取申请者所持有的资源 用于判断持有者是否直接或间接申请申请者的资源
        ArrayList<PageId> applicantPages = getAllTranscationStat(tid);

        //开始寻找是否存在环
        //对每个持有者进行查找
        for(LockStat holder : holders){
            TransactionId transactionId = holder.getTransactionId();
            //去掉T1，因为虽然上图没画出这种情况，但T1可能同时也在其他Page上有读锁，这会影响判断结果
            if (!transactionId.equals(tid)) {
                boolean isWaitting = isWaitting(holder,applicantPages,tid);
                if(isWaitting) return true;
            }

        }

        return false;
    }

    /**
     *在持有者中寻找是否存在正在等待申请者持有的资源（直接或间接）
     * @param holder
     * @param applicantPages
     * @param tid
     * @return
     */
    private boolean isWaitting(LockStat holder, ArrayList<PageId> applicantPages, TransactionId tid) {
        //首先判断当前持有者是否有正在等待的锁申请
        TransactionId holderTransactionId = holder.getTransactionId();
        PageId pageId = waitTable.get(holderTransactionId);
        //不存在等待的资源
        if(pageId==null){
            return false;
        }

        //存在等待的资源 就对申请者持有的资源继续迭代 查看是否存在直接等待申请者的资源
        for(PageId applicantpage : applicantPages){
            //持有者正在等待申请者持有的资源 说明存在死锁
            if(applicantpage.equals(pageId)){
                return true;
            }
        }

        //来到这里 说明不存在直接申请持有者资源的死锁状态，但可能存在间接的
        List<LockStat> lockStats = lockTable.get(pageId);
        //对当前的持有者进行递归的判断
        //如果waitingPage的拥有者们(去掉toRemove)中的某一个正在等待pids中的某一个，说明是tid间接在等待
        if(lockStats==null||lockStats.size()==0) return false;
        for(LockStat lockStat : lockStats){
            TransactionId transactionId = lockStat.getTransactionId();
            if(!transactionId.equals(tid)){
                boolean waitting = isWaitting(lockStat, applicantPages, tid);
                if(waitting){
                    return true;
                }
            }
        }

        return false;
    }


    //锁状态类
    private class LockStat{
        TransactionId transactionId;
        //区分读锁和排他锁
        Permissions permissions;

        public LockStat(TransactionId id,Permissions p){
            this.transactionId = id;
            this.permissions=p;
        }

        public TransactionId getTransactionId() {
            return transactionId;
        }

        public Permissions getPermissions() {
            return permissions;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockStat lockStat = (LockStat) o;
            return Objects.equals(transactionId, lockStat.transactionId) &&
                    permissions == lockStat.permissions;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transactionId, permissions);
        }
    }

}
