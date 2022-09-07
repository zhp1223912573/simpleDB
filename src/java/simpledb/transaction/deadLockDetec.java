package simpledb.transaction;

/**
 * @author zhp
 * @date 2022-03-21 22:38
 * 死锁检测器--采用简单的申请锁次数限制来检测死锁的存在
 * 也可以使用申请锁的时间限制来实现，原理基本相同
 */
public class deadLockDetec {

    private static final int LIMIT_TIME_LOCKAPPLY=3;
    private int limt = LIMIT_TIME_LOCKAPPLY;
    private int count ;

    private deadLockDetec(){
        count=0;
    }

    public static deadLockDetec single(){
         return new deadLockDetec();
    }

    public void deadLockDetect() throws TransactionAbortedException {
        if(count> limt){
            throw new TransactionAbortedException();
        }

        count++;
    }
}
