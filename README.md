## cmu15445 fall2017

### related document

* [schedule](https://15445.courses.cs.cmu.edu/fall2017/schedule.html)

* [projects & homeworks](https://15445.courses.cs.cmu.edu/fall2017/assignments.html)

### project profile

* Project 1 - Buffer Pool

* Project 2 - B+ Tree

* Project 3 - Concurrency Control

* Project 4 - Logging & Recovery

## Project1 - Buffer Pool

### 参考资料

* [Lab1](https://github.com/liu-jianhao/CMU-15-445/tree/master/Lab1-Buffer-Pool)

### 功能 

* 在存储管理器中实现缓冲池。缓冲池负责将物理页面从主存储器来回移动到磁盘。

### 组件

* 可扩展的哈希表
* LRU页面替换政策
* 缓冲池管理器

## Project2 - B+ Tree

### 参考资料

* [lab2](https://github.com/liu-jianhao/CMU-15-445/tree/master/Lab2-B-Tree)

## Project3 - Concurrency control

### 参考资料

* [lab3](https://github.com/liu-jianhao/CMU-15-445/tree/master/Lab3-Concurrency-Control)

## project4

* [lab4](https://github.com/liu-jianhao/CMU-15-445/tree/master/Lab4-Logging-Recovery)


* Because the test code is pretty simple and the code base is not fully funcitonal, problems may no be unveiled.


* Dealing with NewPage log_record in redo phase during log recovery.

-> NewPage log_record do not have info about the newly allocated page.

-> Disk Manager don't ensure the page with same page_id allocated in redo phase.