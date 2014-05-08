txprocesspool
=============

Non-blocking execution of synchronous code with Twisted

Usage example
------------


    #!/usr/bin/env python

    import sys, random

    from time import sleep

    from twisted.internet import reactor
    from twisted.python import log

    from txprocesspool import *

    process_pool = DeferredProcessPool(10)
    q = range(100)

    def mult(x):
        sleep(random.random())
        return x + x

    def execute():
        delay = 0

        d = process_pool.exec_async(mult, q.pop(0))
        d.addCallbacks(log.msg, log.err)

        if q:
            reactor.callLater(delay, execute)

    log.startLogging(sys.stdout)
    reactor.callLater(0, execute)
    reactor.run()
