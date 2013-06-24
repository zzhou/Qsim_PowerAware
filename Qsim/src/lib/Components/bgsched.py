#!/usr/bin/env python

'''Super-Simple Scheduler for BG/L'''
from lib.Components.pwmonitor import HIGH_PRICE, LOW_PRICE
__revision__ = '$Revision: 1788 $'

import logging
import sys
import time
import ConfigParser
import threading
import xmlrpclib
try:
    set()
except:
    from sets import Set as set

import Cobalt.Logging, Cobalt.Util
from Cobalt.Data import Data, DataDict, ForeignData, ForeignDataDict
from Cobalt.Components.base import Component, exposed, automatic, query, locking
from Cobalt.Proxy import ComponentProxy
from Cobalt.Exceptions import ReservationError, DataCreationError, ComponentLookupError
from Cobalt.Statistics import Statistics
from Cobalt.Components.job_opt import *
import Cobalt.SchedulerPolicies

logger = logging.getLogger("Cobalt.Components.scheduler")

SLOP_TIME = 180
DEFAULT_RESERVATION_POLICY = "default"

COMP_QUEUE_MANAGER = "queue-manager"

HIGH = 1
LOW = 2
MID = 0

#AdjEst# 
config = ConfigParser.ConfigParser()
config.read(Cobalt.CONFIG_FILES)

def get_histm_config(option, default):
    try:
        value = config.get('histm', option)
    except ConfigParser.NoOptionError:
        value = default
    return value

running_job_walltime_prediction = get_histm_config("running_job_walltime_prediction", "False")    
if running_job_walltime_prediction in ["True", "true"]:
    running_job_walltime_prediction = True
else:
    running_job_walltime_prediction = False
 #*AdjEst*    

class Reservation (Data):
    
    """Cobalt scheduler reservation."""
    
    fields = Data.fields + [
        "tag", "name", "start", "duration", "cycle", "users", "partitions",
        "active", "queue", 
    ]
    
    required = ["name", "start", "duration"]
    
    def __init__ (self, spec):
        Data.__init__(self, spec)
        self.tag = spec.get("tag", "reservation")
        self.cycle = spec.get("cycle")
        self.users = spec.get("users", "")
        self.createdQueue = False
        self.partitions = spec.get("partitions", "")
        self.name = spec['name']
        self.start = spec['start']
        self.queue = spec.get("queue", "R.%s" % self.name)
        self.duration = spec.get("duration")
        
    def _get_active(self):
        return self.is_active()
    
    active = property(_get_active)
    
    def update (self, spec):
        if spec.has_key("users"):
            qm = ComponentProxy(self.COMP_QUEUE_MANAGER)
            try:
                qm.set_queues([{'name':self.queue,}], {'users':spec['users']}, "hed")
            except ComponentLookupError:
                logger.error("unable to contact queue manager when updating reservation users")
                raise
        # try the above first -- if we can't contact the queue-manager, don't update the users
        Data.update(self, spec)

    
    def overlaps(self, start, duration):
        '''check job overlap with reservations'''
        if start + duration < self.start:
            return False

        if self.cycle and duration >= self.cycle:
            return True

        my_stop = self.start + self.duration
        if self.start <= start < my_stop:
            # Job starts within reservation 
            return True
        elif self.start <= (start + duration) < my_stop:
            # Job ends within reservation 
            return True
        elif start < self.start and (start + duration) >= my_stop:
            # Job starts before and ends after reservation
            return True
        if not self.cycle:
            return False
        
        # 3 cases, front, back and complete coverage of a cycle
        cstart = (start - self.start) % self.cycle
        cend = (start + duration - self.start) % self.cycle
        if cstart < self.duration:
            return True
        if cend < self.duration:
            return True
        if cstart > cend:
            return True
        
        return False

    def job_within_reservation(self, job):
        if not self.is_active():
            return False
        
        if job.queue == self.queue:
            job_end = time.time() + 60 * float(job.walltime) + SLOP_TIME
            if not self.cycle:
                res_end = self.start + self.duration
                if job_end < res_end:
                    return True
                else:
                    return False
            else:
                if 60 * float(job.walltime) + SLOP_TIME > self.duration:
                    return False
                
                relative_start = (time.time() - self.start) % self.cycle
                relative_end = relative_start + 60 * float(job.walltime) + SLOP_TIME
                if relative_end < self.duration:
                    return True
                else:
                    return False
        else:
            return False

    
    def is_active(self, stime=False):
        if not stime:
            stime = time.time()
            
        if stime < self.start:
            return False
        
        if self.cycle:
            now = (stime - self.start) % self.cycle
        else:
            now = stime - self.start    
        if now <= self.duration:
            return True

    def is_over(self):
        # reservations with a cycle time are never "over"
        if self.cycle:
            return False
        
        stime = time.time()
        if (self.start + self.duration) <= stime:
            return True
        else:
            return False
        
        

class ReservationDict (DataDict):
    
    item_cls = Reservation
    key = "name"
    
    def q_add (self, *args, **kwargs):
        qm = ComponentProxy(self.COMP_QUEUE_MANAGER)
        try:
            queues = [spec['name'] for spec in qm.get_queues([{'name':"*"}])]
        except ComponentLookupError:
            logger.error("unable to contact queue manager when adding reservation")
            raise
        
        try:
            reservations = Cobalt.Data.DataDict.q_add(self, *args, **kwargs)
        except KeyError, e:
            raise ReservationError("Error: a reservation named %s already exists" % e)
                
        for reservation in reservations:
            if reservation.queue not in queues:
                try:
                    qm.add_queues([{'tag': "queue", 'name':reservation.queue, 'policy':DEFAULT_RESERVATION_POLICY}], "bgsched")
                except Exception, e:
                    logger.error("unable to add reservation queue %s (%s)" % \
                                 (reservation.queue, e))
                else:
                    reservation.createdQueue = True
                    logger.info("added reservation queue %s" % (reservation.queue))
            try:
                # we can't set the users list using add_queues, so we want to call set_queues even if bgsched
                # just created the queue
                qm.set_queues([{'name':reservation.queue}],
                              {'state':"running", 'users':reservation.users}, "bgsched")
            except Exception, e:
                logger.error("unable to update reservation queue %s (%s)" % \
                             (reservation.queue, e))
            else:
                logger.info("updated reservation queue %s" % reservation.queue)
    
        return reservations
        
    def q_del (self, *args, **kwargs):
        reservations = Cobalt.Data.DataDict.q_del(self, *args, **kwargs)
        qm = ComponentProxy('queue-manager')
        queues = [spec['name'] for spec in qm.get_queues([{'name':"*"}])]
        spec = [{'name': reservation.queue} for reservation in reservations \
                if reservation.createdQueue and reservation.queue in queues and \
                not self.q_get([{'queue':reservation.queue}])]
        try:
            qm.set_queues(spec, {'state':"dead"}, "bgsched")
        except Exception, e:
            logger.error("problem disabling reservation queue (%s)" % e)
        return reservations

class Job (ForeignData):
    
    """A cobalt job."""
    
    fields = ForeignData.fields + [
        "nodes", "location", "jobid", "state", "index", "walltime", "queue", "user", "submittime", 
        "starttime", "project", 'is_runnable', 'is_active', 'has_resources', "score", 'attrs', 
        'walltime_p',   'power'#*AdjEst*  
    ]
    
    def __init__ (self, spec):
        ForeignData.__init__(self, spec)
        spec = spec.copy()
        #print spec
        self.partition = "none"
        self.nodes = spec.pop("nodes", None)
        self.location = spec.pop("location", None)
        self.jobid = spec.pop("jobid", None)
        self.state = spec.pop("state", None)
        self.index = spec.pop("index", None)
        self.walltime = spec.pop("walltime", None)
        self.walltime_p = spec.pop("walltime_p", None)   #*AdjEst*
        self.queue = spec.pop("queue", None)
        self.user = spec.pop("user", None)
        self.submittime = spec.pop("submittime", None)
        self.starttime = spec.pop("starttime", None)
        self.project = spec.pop("project", None)
        self.is_runnable = spec.pop("is_runnable", None)
        self.is_active = spec.pop("is_active", None)
        self.has_resources = spec.pop("has_resources", None)
        self.score = spec.pop("score", 0.0)
        self.attrs = spec.pop("attrs", {})
        self.power = spec.pop("power", 0)
        
        logger.info("Job %s/%s: Found job" % (self.jobid, self.user))

class JobDict(ForeignDataDict):
    item_cls = Job
    key = 'jobid'
    __oserror__ = Cobalt.Util.FailureMode("QM Connection (job)")
    
    __fields__ = ['nodes', 'location', 'jobid', 'state', 'index',
                  'walltime', 'queue', 'user', 'submittime', 'starttime', 'project',
                  'is_runnable', 'is_active', 'has_resources', 'score', 'attrs', 
                  'walltime_p',  'power'#*AdjEst*
                  ]
    def __init__(self, queue_manager_name):
        self.queue_manager_name = queue_manager_name
        self.__function__ = ComponentProxy(self.queue_manager_name).get_jobs

class Queue(ForeignData):
    fields = ForeignData.fields + [
        "name", "state", "policy", "priority"
    ]

    def __init__(self, spec):
        ForeignData.__init__(self, spec)
        spec = spec.copy()
        self.name = spec.pop("name", None)
        self.state = spec.pop("state", None)
        self.policy = spec.pop("policy", None)
        self.priority = spec.pop("priority", 0)
 
    def LoadPolicy(self):
        '''Instantiate queue policy modules upon demand'''
        if self.policy not in Cobalt.SchedulerPolicies.names:
            logger.error("Cannot load policy %s for queue %s" % \
                         (self.policy, self.name))
        else:
            pclass = Cobalt.SchedulerPolicies.names[self.policy]
            self.policy = pclass(self.name)


class QueueDict(ForeignDataDict):
    item_cls = Queue
    key = 'name'
    __oserror__ = Cobalt.Util.FailureMode("QM Connection (queue)")
    #__function__ = ComponentProxy(queue_manager_name).get_queues
    __fields__ = ['name', 'state', 'policy', 'priority']
    def __init__(self, queue_manager_name):
        self.queue_manager_name = queue_manager_name
        self.__function__ = ComponentProxy(queue_manager_name).get_queues

#    def Sync(self):
#        qp = [(q.name, q.policy) for q in self.itervalues()]
#        Cobalt.Data.ForeignDataDict.Sync(self)
#        [q.LoadPolicy() for q in self.itervalues() \
#         if (q.name, q.policy) not in qp]


class BGSched (Component):
    
    implementation = "bgsched"
    name = "scheduler"
    logger = logging.getLogger("Cobalt.Components.scheduler")
    
    _configfields = ['utility_file']
    _config = ConfigParser.ConfigParser()
    _config.read(Cobalt.CONFIG_FILES)
    if not _config._sections.has_key('bgsched'):
        print '''"bgsched" section missing from cobalt config file'''
        sys.exit(1)
    config = _config._sections['bgsched']
    mfields = [field for field in _configfields if not config.has_key(field)]
    if mfields:
        print "Missing option(s) in cobalt config file [bgsched] section: %s" % (" ".join(mfields))
        sys.exit(1)
    if config.get("default_reservation_policy"):
        global DEFAULT_RESERVATION_POLICY
        DEFAULT_RESERVATION_POLICY = config.get("default_reservation_policy")
    
    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self.COMP_QUEUE_MANAGER = "queue-manager"
        self.COMP_SYSTEM = "system"
        self.reservations = ReservationDict()
        self.queues = QueueDict(self.COMP_QUEUE_MANAGER)
        self.jobs = JobDict(self.COMP_QUEUE_MANAGER)
        self.started_jobs = {}
        self.sync_state = Cobalt.Util.FailureMode("Foreign Data Sync")
        self.active = True
        self.get_current_time = time.time
        
        #power monitor
        self.powmonitor=ComponentProxy("powmonitor")
        
        # schedule
        self.schedule_count = 0
        self.refill_count = 0
         
    
    def __getstate__(self):
        return {'reservations':self.reservations, 'version':1,
                'active':self.active}
    
    def __setstate__(self, state):
        self.reservations = state['reservations']
        if 'active' in state:
            self.active = state['active']
        else:
            self.active = True
        
        self.queues = QueueDict(self.COMP_QUEUE_MANAGER)
        self.jobs = JobDict(self.COMP_QUEUE_MANAGER)
        self.started_jobs = {}
        self.sync_state = Cobalt.Util.FailureMode("Foreign Data Sync")
        
        self.get_current_time = time.time
        self.lock = threading.Lock()
        self.statistics = Statistics()

    # order the jobs with biggest utility first
    def utilitycmp(self, job1, job2):
        return -cmp(job1.score, job2.score)
    
    # order the jobs with biggest size first
    def sizecmp(self, job1, job2):
        return -cmp(job1.nodes, job2.nodes)
    
    # get total power consumption of job considering nodes and walltime
    def get_total_power(self, job):
        node = int(job.nodes)
        power = int(job.power)
        rack = float(node) / 1024
        
        total_power = float(power*rack)
        return total_power
    
    # order the jobs with the lowest power consumption first
    def powercmp_asc(self, job1, job2):
        power1 = self.get_total_power(job1)
        power2 = self.get_total_power(job2)
        return cmp(power1, power2)
    
    #order the jobs with the most power consumption first
    def powercmp_desc(self, job1, job2):
        power1 = self.get_total_power(job1)
        power2 = self.get_total_power(job2)
        return -cmp(power1, power2)
    
    def prioritycmp(self, job1, job2):
        """Compare 2 jobs first using queue priority and then first-in, first-out."""
        
        val = cmp(self.queues[job1.queue].priority, self.queues[job2.queue].priority)
        if val == 0:
            return self.fifocmp(job1, job2)
        else:
            # we want the higher priority first
            return -val
        
    def fifocmp (self, job1, job2):
        """Compare 2 jobs for first-in, first-out."""
        
        def fifo_value (job):
            if job.index is not None:
                return int(job.index)
            else:
                return job.jobid
            
        # Implement some simple variations on FIFO scheduling
        # within a particular queue, based on queue policy
        fifoval = cmp(fifo_value(job1), fifo_value(job2))
        if(job1.queue == job2.queue):
            qpolicy = self.queues[job1.queue].policy
            sizeval = cmp(int(job1.nodes), int(job2.nodes))
            wtimeval = cmp(int(job1.walltime), int(job2.walltime))
            if(qpolicy == 'largest-first' and sizeval):
                return -sizeval
            elif(qpolicy == 'smallest-first' and sizeval):
                return sizeval
            elif(qpolicy == 'longest-first' and wtimeval):
                return -wtimeval
            elif(qpolicy == 'shortest-first' and wtimeval):
                return wtimeval
            else:
                return fifoval
        else:
            return fifoval

        return cmp(fifo_value(job1), fifo_value(job2))

    def save_me(self):
        Component.save(self)
    save_me = automatic(save_me)

    def add_reservations (self, specs, user_name):
        self.logger.info("%s adding reservation: %r" % (user_name, specs))
        return self.reservations.q_add(specs)
    add_reservations = exposed(query(add_reservations))

    def del_reservations (self, specs, user_name):
        self.logger.info("%s releasing reservation: %r" % (user_name, specs))
        return self.reservations.q_del(specs)
    del_reservations = exposed(query(del_reservations))

    def get_reservations (self, specs):
        return self.reservations.q_get(specs)
    get_reservations = exposed(query(get_reservations))

    def set_reservations(self, specs, updates, user_name):
        self.logger.info("%s modifying reservation: %r with updates %r" % (user_name, specs, updates))
        def _set_reservations(res, newattr):
            res.update(newattr)
        return self.reservations.q_get(specs, _set_reservations, updates)
    set_reservations = exposed(query(set_reservations))

    def check_reservations(self):
        ret = ""
        reservations = self.reservations.values()
        for i in range(len(reservations)):
            for j in range(i+1, len(reservations)):
                # if at least one reservation is cyclic, we want *that* reservation to be the one getting its overlaps method
                # called
                if reservations[i].cycle is not None:
                    res1 = reservations[i]
                    res2 = reservations[j]
                else:
                    res1 = reservations[j]
                    res2 = reservations[i]

                # we subtract a little bit because the overlaps method isn't really meant to do this
                # it will report warnings when one reservation starts at the same time another ends
                if res1.overlaps(res2.start, res2.duration - 0.00001):
                    # now we need to check for overlap in space
                    results = ComponentProxy(self.COMP_SYSTEM).get_partitions(
                        [ {'name': p, 'children': '*', 'parents': '*'} for p in res2.partitions.split(":") ]
                    )
                    for p in res1.partitions.split(":"):
                        for r in results:
                            if p==r['name'] or p in r['children'] or p in r['parents']:
                                ret += "Warning: reservation '%s' overlaps reservation '%s'\n" % (res1.name, res2.name)

        return ret
    check_reservations = exposed(check_reservations)

    def sync_data(self):
        started = self.get_current_time()
        for item in [self.jobs, self.queues]:
            try:
                item.Sync()
            except (ComponentLookupError, xmlrpclib.Fault):
                # the ForeignDataDicts already include FailureMode stuff
                pass
        # print "took %f seconds for sync_data" % (time.time() - started, )
    #sync_data = automatic(sync_data)

    def _run_reservation_jobs (self, reservations_cache):
        # handle each reservation separately, as they shouldn't be competing for resources
        for cur_res in reservations_cache.itervalues():
            queue = cur_res.queue
            if not (self.queues.has_key(queue) and self.queues[queue].state == 'running'):
                continue
            
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue}])
            active_jobs = []
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid) and cur_res.job_within_reservation(j):
                    active_jobs.append(j)
    
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            
            job_location_args = []
            for job in active_jobs:
                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'required': cur_res.partitions.split(":"),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'attrs': job.attrs,
                    } )

            # there's no backfilling in reservations
            try:
                best_partition_dict = ComponentProxy(self.COMP_SYSTEM).find_job_location(job_location_args, [])
            except:
                self.logger.error("failed to connect to system component")
                best_partition_dict = {}
    
            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self._start_job(job, best_partition_dict[jobid])

    def _start_job(self, job, partition_list):
        cqm = ComponentProxy(self.COMP_QUEUE_MANAGER)
        
        try:
            self.logger.info("trying to start job %d on partition %r" % (job.jobid, partition_list))
            cqm.run_jobs([{'tag':"job", 'jobid':job.jobid}], partition_list)
        except ComponentLookupError:
            self.logger.error("failed to connect to queue manager")
            return

        self.started_jobs[job.jobid] = self.get_current_time()

    def schedule_jobs (self):
        '''look at the queued jobs, and decide which ones to start'''

        #started_scheduling = time.time()

        if not self.active:
            return
        
        self.sync_data()
        
        # if we're missing information, don't bother trying to schedule jobs
        if not (self.queues.__oserror__.status and self.jobs.__oserror__.status):
            self.sync_state.Fail()
            return
        self.sync_state.Pass()
        
        self.lock.acquire()
        try:
            # cleanup any reservations which have expired
            for res in self.reservations.values():
                if res.is_over():
                    self.logger.info("reservation %s has ended; removing" % res.name)
                    self.reservations.q_del([{'name': res.name}])
    
            reservations_cache = self.reservations.copy()
        except:
            # just to make sure we don't keep the lock forever
            self.logger.error("error in schedule_jobs", exc_info=True)
        self.lock.release()
        
        # clean up the started_jobs cached data
        now = self.get_current_time()
        for job_name in self.started_jobs.keys():
            if (now - self.started_jobs[job_name]) > 60:
                del self.started_jobs[job_name]

        active_queues = []
        spruce_queues = []
        res_queues = set()
        for item in reservations_cache.q_get([{'queue':'*'}]):
            if self.queues.has_key(item.queue):
                if self.queues[item.queue].state == 'running':
                    res_queues.add(item.queue)

        for queue in self.queues.itervalues():
            if queue.name not in res_queues and queue.state == 'running':
                if queue.policy == "high_prio":
                    spruce_queues.append(queue)
                else:
                    active_queues.append(queue)
        
        # handle the reservation jobs that might be ready to go
        self._run_reservation_jobs(reservations_cache)

        # figure out stuff about queue equivalence classes
#        res_info = {}
#        for cur_res in reservations_cache.values():
#            res_info[cur_res.name] = cur_res.partitions
#        try:
#            equiv = ComponentProxy(self.COMP_SYSTEM).find_queue_equivalence_classes(res_info, [q.name for q in active_queues + spruce_queues])
#        except:
#            self.logger.error("failed to connect to system component")
#            return
        #for simulator performance
        equiv= [{'reservations': [], 'queues': ['default']}]
        
        util = self.getUtilizationRate()        
        if util == 1.0:
            return
        
        for eq_class in equiv:
            # recall that is_runnable is True for certain types of holds
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in active_queues \
                if queue.name in eq_class['queues']])
            
            #no waiting job, skip the scheduling iteration for this eq_class
            if len(temp_jobs) == 0:
                continue
            
            active_jobs = []
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    active_jobs.append(j)
            
            temp_jobs = self.jobs.q_get([{'is_runnable':True, 'queue':queue.name} for queue in spruce_queues \
                if queue.name in eq_class['queues']])
            spruce_jobs = []
            for j in temp_jobs:
                if not self.started_jobs.has_key(j.jobid):
                    spruce_jobs.append(j)
    
            # if there are any pending jobs in high_prio queues, those are the only ones that can start
            if spruce_jobs:
                active_jobs = spruce_jobs

            # get the cutoff time for backfilling
            #
            # BRT: should we use 'has_resources' or 'is_active'?  has_resources returns to false once the resource epilogue
            # scripts have finished running while is_active only returns to false once the job (not just the running task) has
            # completely terminated.  the difference is likely to be slight unless the job epilogue scripts are heavy weight.
            temp_jobs = [job for job in self.jobs.q_get([{'has_resources':True}]) if job.queue in eq_class['queues']]
            end_times = []
            for job in temp_jobs:
                # take the max so that jobs which have gone overtime and are being killed
                # continue to cast a small backfilling shadow (we need this for the case
                # that the final job in a drained partition runs overtime -- which otherwise
                # allows things to be backfilled into the drained partition)
                if self.running_job_walltime_prediction:
                    runtime_estimate = float(job.walltime_p)
                else:
                    runtime_estimate = float(job.walltime)
                
                end_time = max(float(job.starttime) + 60 * runtime_estimate, now + 5*60)   ##*AdjEst*
                #end_time = max(float(job.starttime) + 60 * float(job.walltime), now + 5*60)
                
                end_times.append([job.location, end_time])
                
            for res_name in eq_class['reservations']:
                cur_res = reservations_cache[res_name]

                if not cur_res.cycle:
                    end_time = float(cur_res.start) + float(cur_res.duration)
                else:
                    done_after = float(cur_res.duration) - ((now - float(cur_res.start)) % float(cur_res.cycle))
                    if done_after < 0:
                        done_after += cur_res.cycle
                    end_time = now + done_after
                if cur_res.is_active():
                    for part_name in cur_res.partitions.split(":"):
                        end_times.append([[part_name], end_time])
    
            
            if not active_jobs:
                continue
            active_jobs.sort(self.utilitycmp)
            
            ''' Resort jobs within window '''
#            if self.checkPowerAware():
#                #window_size = ComponentProxy(self.COMP_SYSTEM).get_window_size()  
#                window_size = len(active_jobs)
#                # window size for job resorting
#                active_jobs_top10 = active_jobs[0:int(window_size)]
#                active_jobs_rest = active_jobs[int(window_size):len(active_jobs)]
#                
#                price_level = self.checkPriceLevel()
#                
#                if price_level == HIGH:
#                    #reorganize jobs
#                    active_jobs_top10.sort(self.powercmp_asc)    
#                elif price_level == LOW:
#                    active_jobs_top10.sort(self.powercmp_desc)
#                    
#                active_jobs = active_jobs_top10
#                active_jobs.extend(active_jobs_rest)
            
            power_aware = self.checkPowerAware()
            
            if power_aware:
                price_level = self.checkPriceLevel()
                if price_level == HIGH:
                    if not self.check_power_budget_available(active_jobs):
                        return                
            
#            if self.checkPowerAware():
#                log_queue_length = self.get_queue_length_log_title()
#                pre_length = len(active_jobs)
#                head_job = active_jobs[0]
#                
#                if self.checkPowerAware() == HIGH:
#                    active_jobs.sort(self.powercmp_asc)
#                    position = active_jobs.index(head_job)
#                    trunc_active_jobs = active_jobs[0: position+1]
#                    active_jobs = trunc_active_jobs
#                else:
#                    active_jobs.sort(self.powercmp_desc)
#                    position = active_jobs.index(head_job)
#                    trunc_active_jobs = active_jobs[0:position+1]
#                    active_jobs = trunc_active_jobs
#                post_length = len(active_jobs)
            
#            ''' Log job info '''
#            log_file = self.get_schedule_log_title()+ "_" + str(power_aware) +".list"
#            log_str = ""
#            for job in active_jobs:
#                log_str = log_str + "{jobid %s nodes %s power %s walltime %s}\n" % (str(job.jobid), str(job.nodes), str(job.power), str(job.walltime))
#            self.log_info(self.getCurrentDateTime(), log_file)
#            self.log_info(log_str, log_file)
                
            best_partition_dict = {}
            # now smoosh lots of data together to be passed to the allocator in the system component
            job_location_args = []
            
            for job in active_jobs:
                forbidden_locations = set()
                for res_name in eq_class['reservations']:
                    cur_res = reservations_cache[res_name]
                    if cur_res.overlaps(self.get_current_time(), 60 * float(job.walltime) + SLOP_TIME):
                        forbidden_locations.update(cur_res.partitions.split(":"))

                job_location_args.append( 
                    { 'jobid': str(job.jobid), 
                      'nodes': job.nodes, 
                      'queue': job.queue, 
                      'forbidden': list(forbidden_locations),
                      'utility_score': job.score,
                      'walltime': job.walltime,
                      'walltime_p': job.walltime_p,  #*AdjEst*
                      'attrs': job.attrs,
                    })

            try:
                while len(job_location_args) != 0:
                    best_partition = ComponentProxy(self.COMP_SYSTEM).find_job_location(job_location_args, end_times)
                    
                    if best_partition:
                        best_partition_dict.update(best_partition)
                        for jobid in best_partition:
                            for job_arg in job_location_args:
                                if jobid == job_arg['jobid']:
                                    job_location_args.remove(job_arg)
                                    break
                    else:
                        break       
            except:
                self.logger.error("failed to connect to system component", exc_info=True)
                best_partition_dict = {}
            
            
            # release reserved partition
            for jobid in best_partition_dict:
                for partition in best_partition_dict[jobid]:
                    self.release_partition(partition)
            
            if power_aware:
                price_level = self.checkPriceLevel()
                
                if price_level == HIGH:
                    rest_power_budget = self.getRestPowerBudget()
                    inlist, exlist = find_optimal_solu_DP(int(rest_power_budget), best_partition_dict, self.jobs)
                # renew selected jobs
                    best_partition_dict = inlist
                    for jobid in best_partition_dict:
                        get_power(self.jobs[int(jobid)])
                
            #print "%f: took %f seconds for finding job locations" % (self.get_current_time(), time.time() - started_find_job_location )
#            if self.checkPowerAware():
#                log_str_queue_length = "%s pre_length: %d post_length: %d selected_job: %d" % (self.getCurrentDateTime(), pre_length, post_length, len(best_partition_dict))
#                self.log_info(log_str_queue_length, log_queue_length)
            
            
            # start each job                                
            for jobid in best_partition_dict:
                job = self.jobs[int(jobid)]
                self._start_job(job, best_partition_dict[jobid])
            
            ComponentProxy("event-manager").set_go_next(True)
            self.schedule_count += 1
#            print "------------------------------------------------------------------------"
                  
            #print "%f: took %f seconds for scheduling loop" % (self.get_current_time(), time.time() - started_scheduling, )
    schedule_jobs = locking(automatic(schedule_jobs))
    
    def enable(self, user_name):
        """Enable scheduling"""
        self.logger.info("%s enabling scheduling", user_name)
        self.active = True
    enable = exposed(enable)

    def disable(self, user_name):
        """Disable scheduling"""
        self.logger.info("%s disabling scheduling", user_name)
        self.active = False
    disable = exposed(disable)
    
    def log_info(self, spec, filename):
        
        f=open(filename, "a")

        print >> f, spec
        f.close()
        return True
    
    def release_partition(self, partition):
        return ComponentProxy(self.COMP_SYSTEM).release_partition(partition)
    release_partition = exposed(release_partition)
    
    def get_schedule_count(self):
        return self.schedule_count
    get_schedule_count = exposed(get_schedule_count)
    
    def get_refill_count(self):
        return self.refill_count
    get_refill_count = exposed(get_refill_count)

    def checkPowerAware(self):
        return ComponentProxy(self.COMP_SYSTEM).is_power_aware()
    
    def checkPriceLevel(self):
        price = self.powmonitor.get_current_price_level()
        if price == HIGH_PRICE:
            return HIGH
        elif price == LOW_PRICE:
            return LOW
        else:
            return MID
    
    def getPowerBudget(self):
        return ComponentProxy(self.COMP_SYSTEM).get_power_budget()
    
    def getRestPowerBudget(self):
        return ComponentProxy(self.COMP_SYSTEM).get_rest_power_budget()
    
    def getWindowSize(self):
        return ComponentProxy(self.COMP_SYSTEM).get_window_size()
    
    def getRunningPowerUsage(self):
        return ComponentProxy(self.COMP_SYSTEM).get_running_job_power_usage()
    
    def getUtilizationRate(self):
        return ComponentProxy(self.COMP_SYSTEM).get_utilization_rate()
    
    def getRunningJobs(self):
        return ComponentProxy(self.COMP_SYSTEM).get_running()
    
    def getCurrentDateTime(self):
        return ComponentProxy("event-manager").get_current_date_time()
    
    def getSimStart(self):
        return ComponentProxy(self.COMP_SYSTEM).get_sim_start()
    
    def getSimEnd(self):
        return ComponentProxy(self.COMP_SYSTEM).get_sim_end()
    
    def get_schedule_log_title(self):
        start_date = str(self.getSimStart())
        end_date = str(self.getSimEnd())
        
        return ("schedule_stats_" +
                start_date + "_" +
                end_date)
        
    def get_queue_length_log_title(self):
        start_date = str(self.getSimStart())
        end_date = str(self.getSimEnd())
        
        return ("queue_length_" +
                start_date + "_" +
                end_date)
    
    def get_idle_nodes(self):
        return ComponentProxy(self.COMP_SYSTEM).get_idle_nodes() / 512
    
    def check_power_budget_available(self, active_jobs):
        rest_power = self.getRestPowerBudget()
        
        if rest_power <= 0:
            return False
        
        for job in active_jobs:
            if get_power(job) <= rest_power:
                return True
        
        return False
        