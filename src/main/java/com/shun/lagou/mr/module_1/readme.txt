本次为mr模块一作业:
作业分析:
    1.输入为3个小文件,可使用CombineTextInputFormat类来实现合并输入文件达到优化目的(节省创建MapTask的资源)
    2.利用mr自带的对key排序来达到目的(节省计算资源)
    3.可以只生成一个分区(节省带宽资源)
个人作业总结:
    mr编程主要是根据不同的业务场景对InputFormat,Mapper,Partitioner,Combiner,Reducer,OutputFormat六个类有选择的实现各自逻辑,优秀的
解题思路需要我们对整个流程和各自的功能有清晰的理解
