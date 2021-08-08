package com.shun.lagou.mr.module_1.segment_lock;

import java.util.LinkedList;


public class DoubleBuffer {
    /**
     * 内存1
     */
    LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();
    /**
     * 内存2
     */
    LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

    /**
     * 把数据写到当前内存
     *
     * @param log
     */
    public void write(EditLog log) {
        currentBuffer.add(log);
    }

    /**
     * 两个内存交换数据
     */
    public void setReadyToSync() {
        LinkedList<EditLog> tmp = currentBuffer;
        currentBuffer = syncBuffer;
        syncBuffer = tmp;
    }

    /**
     * 获取当前正在刷磁盘的内存里的ID最大值
     * @return
     */
    public Long getSyncMaxTxid(){
        return syncBuffer.getLast().txid;
    }

    /**
     * 就是把数据写到磁盘上面
     * 为了演示效果,所以我们只是打印出来
     */
    public void flush(){
        for (EditLog editLog : syncBuffer) {
            System.out.println(editLog);
        }
        syncBuffer.clear();
    }

}
