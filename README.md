SimpleDb是MIT的研究生课程的作业项目，它包含了数据库的存储、算子、优化、事务、索引、日志恢复等相关实现，全方位介绍了如何从零开始实现一个数据库。项目共分为6个lab，分别实现上述各模块。



1）lab1：主要实现数据库的存储结构，包括 表、文件、页、记录、字段，以及缓冲池等，实现记录的顺序存储。

2）lab2：主要实现sql最常用的数据库的算子开发（针对int和string数据类型的字段），包括filter算子、aggregation算子（count/sum/avg/max/min）、表的连接（join）算子；以及记录的插入和删除操作。

3）lab3：主要实现了SQL优化器，可分为代价估计和Join优化两部分。利用直方图进行谓词预估统计，来估计不同查询计划的cost；然后利用left-deep-tree和动态规划算法进行Join优化。

4）lab4：主要实现事务管理，实现了遵从二阶段锁协议的页级别共享锁和排他锁，做到多个事务之间的互不干扰访问；并且实现了死锁检测与解决。

5）lab5：主要实现B+树索引，主要有查询、插入、删除等功能。

6）lab6：主要实现了redo log和undo log日志系统，以支持回滚和崩溃恢复。
