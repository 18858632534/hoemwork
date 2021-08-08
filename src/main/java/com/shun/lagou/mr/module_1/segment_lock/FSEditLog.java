package com.shun.lagou.mr.module_1.segment_lock;

public class FSEditLog {
    private long txid = 0L;
    private DoubleBuffer editLogBuffer = new DoubleBuffer();
    private volatile Boolean isSYncRunning = false;
    private volatile Boolean isWaitSync = false;
    private volatile Long syncMaxTxid = 0L;

    /**
     * 一个线程 就会有自己的一个ThreadLocal的副本
     */
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

    public static void main(String[] args) {

    }

    /**
     * 写元数据日志的方法
     *
     * 线程1,
     * 顺序
     *
     * hadoop fs mkdir /data 1
     * hadoop fs delete /data 2
     * @param content
     */
    public void logEdit(String content){ //mkdir /data
        synchronized (this){
            //日志的ID号,元数据信息的ID号
            txid++;

            /**
             * 每个线程都会有自己的一个副本
             * 线程1 1
             * 线程2 2
             * 线程3 3
             */
            localTxid.set(txid);

            EditLog log = new EditLog(txid,content);
            // 往内存里面写数据
            editLogBuffer.write(log);
        }//释放锁

        /**
         * 内存1:
         * 线程1,1 元数据1
         * 线程2,2 元数据2
         * 线程3,3 元数据3
         */
        logSync();

    }
    private void logSync(){
        /**
         * 线程1,ID号:1
         */

        synchronized (this){
            //当前是否正在往磁盘写数据,默认是false
            //这个值为true
            if(isSYncRunning){

                //当前线程的副本,当前的元数据信息编号就是2
                long txid = localTxid.get();

                // 当前线程编号如果小于 正在刷写的最大的
                if(txid <= syncMaxTxid){
                    return ;
                }

                if(isWaitSync){
                    //直接返回
                    return ;
                }

                //重新赋值
                isWaitSync = true;
                while(isSYncRunning){
                    try {
                        //线程4就会在这里等待
                        //释放锁
                        /**
                         * 时间到了
                         * 被唤醒了
                         */
                        wait(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
            /**
             * 交换内存,我是直接交换的内存,肯定是简单粗暴
             * 真正的源码里面是有判断的
             * 如果来不来就直接交换内存,频繁的交换内存,是很影响性能的.
             */
            editLogBuffer.setReadyToSync();

            if(editLogBuffer.currentBuffer.size()>0){
                //获取当前 内存2(正在往磁盘上面写数据的那个内存)
                //里面元数据日志编号最大的是多少

                syncMaxTxid = editLogBuffer.getSyncMaxTxid();
            }

            isSYncRunning=true;
        } //释放锁

        //往磁盘上面写数据(这个操作是很耗费时间的)

        /**
         * 线程一 执行如下代码
         *
         * 在最耗费时间的这段代码上面是没有加锁的
         * 几毫秒,几十毫秒
         */
        editLogBuffer.flush(); //然后就写完了

        synchronized (this){
            //状态恢复
            isSYncRunning = false;
            //唤醒当前wait的线程
            notify();
        }

    }
}
