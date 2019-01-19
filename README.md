## cmu15445 fall2017

### related document

* [schedule](https://15445.courses.cs.cmu.edu/fall2017/schedule.html)

* [projects & homeworks](https://15445.courses.cs.cmu.edu/fall2017/assignments.html)

### project4

Because the test code is pretty simple and the code base is not fully funcitonal, problems may no be unveiled.


* Dealing with NewPage log_record in redo phase during log recovery.

-> NewPage log_record do not have info about the newly allocated page.

-> Disk Manager don't ensure the page with same page_id allocated in redo phase.