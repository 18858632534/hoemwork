package com.shun.lagou.mr.module_1.segment_lock;

public class EditLog {
    /**
     * 日志的编号,递增,并且是唯一的
     */
    long txid;
    /**
     * 日志的内容
     */
    String context;

    public EditLog(long txid, String context) {
        this.txid = txid;
        this.context = context;
    }

    /**
     * 方便我们打印日志
     * @return
     */
    @Override
    public String toString() {
        return "EditLog{" +
                "txid=" + txid +
                ", context='" + context + '\'' +
                '}';
    }

}
