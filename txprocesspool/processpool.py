import multiprocessing

from twisted.internet import defer, reactor
from time import time


class ExceptionWrapper(object):
    """
    Wrapper for :class:`Exception` objects to make them
    picklable.
    """

    def __init__(self, ex):
        self._ex = ex

    def __reduce__(self):
        reduced_exception = self._ex.__reduce__()

        if len(reduced_exception) == 3:
            (ex_obj, ex_args, ex_kwargs) = reduced_exception
        else:
            (ex_obj, ex_args), ex_kwargs = reduced_exception, {}

        wrapper_func = _ExceptionWrapperRebuilder(ex_obj)

        return (wrapper_func, (ex_args, ex_kwargs))

    def __getattr__(self, name):
        return getattr(self._ex, name)


class _ExceptionWrapperRebuilder(object):
    """
    Internal class used by :class:`ExceptionWrapper` to rewrap
    :class:`Exception` objects when they are depickled.

    A function defined inside :meth:`ExceptionWrapper.__reduce__` which takes
    :attr:`ex_obj` from its scope cannot be used, as functions must be
    defined at the module's top level to be picklable.
    """

    def __init__(self, inner_obj):
        self._inner_obj = inner_obj

    def __call__(self, *args):
        nonkwargs, kwargs = args

        ex = self._inner_obj(*nonkwargs, **kwargs)
        return ExceptionWrapper(ex)


class ProcessPollError(Exception): pass


class DeferredProcessPool(object):
    def __init__(self, max_tasks=2, timeout=5):
        self.timeout = timeout
        self.running_tasks = 0
        self.semaphore = defer.DeferredSemaphore(max_tasks)

    def _exec(self, fun, child_pipe, *args, **kwargs):
        try:
            result = fun(*args, **kwargs)
            child_pipe.send(result)
        except Exception as err:
            print "Sent in pipe:", err
            child_pipe.send(ExceptionWrapper(err))
        finally:
            child_pipe.close()

    def _check_task(self, proc, pipe):
        # check if child sent anything into pipe
        if not pipe.poll():
            return defer.fail(ProcessPollError("Process didn't returned result"))

        exec_status = defer.succeed

        # child process sent some data into pipe. get it
        result = pipe.recv()

        if isinstance(result, ExceptionWrapper):
            result = result._ex
            exec_status = defer.fail

        # close pipe & cleanup process
        pipe.close() or proc.join()

        # decrement current running tasks counter
        self.running_tasks -= 1

        return exec_status(result)

    def _terminate_task(self, proc, pipe):
        pipe.close()
        proc.terminate() or proc.join()

        self.running_tasks -= 1

    def poll_task(self, proc, pipe_parent, timeout):
        d_manager = defer.Deferred()
        poll_untill = int(time()) + timeout

        def poll():
            d = self._check_task(proc, pipe_parent)
            d.addCallbacks(d_manager.callback, fail)

        def fail(failure):
            if time() > poll_untill:
                self._terminate_task(proc, pipe_parent)
                d_manager.errback(failure)
            elif failure.check(ProcessPollError):
                reactor.callLater(0.1, poll)
            else:
                d_manager.errback(failure)

        # start child process
        proc.start()

        # and poll process status
        poll()

        return d_manager

    def exec_async(self, fun, *args, **kwargs):
        return self.semaphore.run(self.execute, fun, *args, **kwargs)

    def execute(self, fun, *args, **kwargs):
        d = defer.Deferred()

        # open unidirectional pipe to receive result from child process
        pipe_parent, pipe_child = multiprocessing.Pipe()
        proc = multiprocessing.Process(target=self._exec, args=(fun, pipe_child) + args, kwargs=kwargs)

        task_scheduller = self.poll_task(proc, pipe_parent, self.timeout)
        task_scheduller.addCallbacks(d.callback, d.errback)

        return d