Replicated Concurrency Control project

Sushant Bhadkamkar    N18906656 sushant@nyu.edu
Ashwath Pratap Singh  N10341445 ashwath@nyu.edu

Note: We have simulated the database with multiple processes (sites and
transaction manager) for extra credits.

Dependencies: Python 2.7

Steps to run:
1) Start site processes: (ensure ports 9090 - 9099 are open)
    $ ./start_sites.sh
2) Start transaction manager (ensure port 7777 is open)
    $ python transaction_manager.py
3) Run DB client (Neo) by supplying a test file
    $ python neo.py <path-to-test-file>
4) Stop sites:
    $ ./stop_sites.sh
5) Stop transaction manager
    $ Ctrl-C the running process
6) Run 1-5 again for next test case

Please send us an email if you have any questions  
