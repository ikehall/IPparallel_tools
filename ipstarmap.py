from IPython.parallel import Client
import time

def n_queued_jobs(view):
    return view.queue_status()['unassigned']

def starmap(func, iterable, **kwargs):
    """
    A dynamic load balancing parallel implementation of itertools.starmap for IPython.parallel.
    
    The reason for it's existence was twofold.
    First, the desire to easily submit a 'map' onto inputs
      already grouped in tuples in IPython.parallel.
    Second was the ability to submit a 'map' onto very large
      sequences.  Potentially infinite sequences.
    This function allows one to do that.  It is a generator function, so it is iterable.
    It maintains an internal list of returned results that are removed once yielded.
    The iterable passed as an argument need only have a next() method and raise StopIteration
      when it is finished iterating.

    Arguments
    ---------
    *func*   -   The function to be called (remotely) on each iterable.next()
    *iterable* - An iterable, generator, generator function...etc.  Something with a .next() that
                 will raise StopIteration when finished
    *profile*  -  (optional keyword argument.  Default = None) The ipython parallel cluster profile.
                  This function expects the cluster to already be 'up'.  Under the default of None,
                  this will start a client and load balanced view under the default profile, if
                  possible.  If the profile specified is not running, an IO error will be raised.
                  (Ignored if client keyword argument is specified)
    *client*   -  (optional keyword argument.  Default = None) An instance of
                  IPython.parallel.Client
    *max_fill*  - (optional keyword argument.  Default = 500000)The maximum number of
                  'jobs' to submit to the cluster before waiting for earlier jobs to finish.
    *wait*      - (optional keyword argument.  Default = 1)  Number of seconds to wait when
                  submission queue is full, and no further output may be yielded.
    *kwargs*    - Additional keyword arguments are treated as keyword arguments to func.


    A note on the profile and client keyword arguments:  If client is specified, the profile
    kwarg will be ignored.
    
    """
    profile = kwargs.pop('profile', None)
    rc = kwargs.pop('client', None)
    max_fill = kwargs.pop('max_fill', 50000)
    wait = kwargs.pop('wait',1)
    if rc is None:
        rc = Client(profile=profile)
    elif not isinstance(rc, Client):
        raise ValueError('client keyword value expected an instance of IPython.parallel.Client')
    lbv = rc.load_balanced_view()
    
    async_results_list = [] #This will serve as our output queue

    while True:  #GO until StopIteration is raised
        
        if n_queued_jobs(lbv) < max_fill:
            #If there are less than the maximum number of jobs waiting to run,
            #submit the next job, unless we cannot.
            try:
                async_results_list.append(lbv.apply(func, *iterable.next(), **kwargs))
                
            except StopIteration:
                if len(async_results_list) == 0:
                    raise
            
           
        while len(async_results_list) > 0 and async_results_list[0].ready():
            #If there are results ready to be read, pop them off
            yield async_results_list.pop(0).get()
                
        if n_queued_jobs(lbv) >= max_fill:
            time.sleep(wait)
