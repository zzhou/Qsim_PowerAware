#!/usr/bin/env python

'''
Created on Dec 28, 2011

@author: zhouzhou
'''
import time
import logging

from datetime import datetime
from Cobalt.Components.qsim_base import PBSlogger
from Cobalt.Proxy import ComponentProxy
from Cobalt.Components.base import Component, exposed


logging.basicConfig()
logger = logging.getLogger('powmon')

HIGH_PRICE = 3
LOW_PRICE = 1

def sec_to_date(sec, dateformat="%Y-%m-%d %H:%M:%S"):
    tmp = datetime.fromtimestamp(sec)
    fmtdate = tmp.strftime(dateformat)
    return fmtdate    
                      
def date_to_sec(fmtdate, dateformat="%m/%d/%Y %H:%M:%S"):
    t_tuple = time.strptime(fmtdate, dateformat)
    sec = time.mktime(t_tuple)
    return sec

class PowerMonitor(Component):
    """ Power Monitor. Monitor runtime power consumption, logging into output log or provide to scheduler for power-aware job scheduling"""

    implementation = "powmonitor"
    name = "powmonitor"

    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self.event_manager = ComponentProxy("event-manager")
        self.bqsim = ComponentProxy("queue-manager")
        self.powmon_logger = None
        self.total_cost = 0.0
        self.time_power_list =[{"unixtime":0, "power":0, "count":0, "utilization":0}]
        
    def init_powmon_logger(self):
        if self.iomon_logger == None:
            self.iomon_logger = PBSlogger(self.bqsim.get_outputlog_string() + "-powmon")
            
    def get_log_title(self):
        ''' generate a log file title '''
        power_budget = self.bqsim.get_power_budget()
        return ("BGP_power_aware_"+
               str(self.bqsim.is_power_aware())+
               "_power_budget_"+ str(power_budget)+
               "_refill_"+
               str(self.bqsim.get_refill())+"_"+
               str(self.bqsim.get_sim_start())+"_"+
               str(self.bqsim.get_sim_end())+
               ".list")   
            
    def log_avg_info(self, spec):
        ''' log average power and utilization information '''
        power_budget = self.bqsim.get_power_budget()
        
        print "power budget: ", power_budget
        f=open("BGP_avg_power_"+ self.get_log_title(), "a")
        
        print "Length: ", len(spec) 
        for item in spec:
            print >> f, "unixtime", item["unixtime"], "power", item["power"], "utilization", item["utilization"]
        f.close()
        return True
    
    def log_info1(self, spec, filename):
        ''' log a certain string into file specified by filename '''
        f=open(filename, "a")

        print >> f, spec
        f.close()
        return True 
    
    def insert_tag(self, tag):
        ''' insert power & util tag into timeline '''
        pos = len(self.time_power_list)
        
        while tag["unixtime"] < self.time_power_list[pos-1].get("unixtime"):
            pos = pos - 1
        
        if tag["unixtime"] == self.time_power_list[pos-1].get("unixtime"):
            pos = pos -1
            
            # compute average power consumption
            power = self.time_power_list[pos].get("power")
            count = self.time_power_list[pos].get("count")
            
            power_avg = (power*count+tag["power"]) / (count+1)
            
            self.time_power_list[pos]["power"] = int(power_avg)
            
            # compute average utilization
            util = self.time_power_list[pos].get("utilization")
            
            util_avg = (util*count+tag["utilization"]) / (count+1)
            
            self.time_power_list[pos]["count"]= count+1
            self.time_power_list[pos]["utilization"] = util_avg
             
        else:    
            self.time_power_list.insert(pos, {"unixtime":tag["unixtime"], "power":tag["power"],"count":1,"utilization":tag["utilization"]})
        
        return pos
    
    def get_current_price_level(self):
        ''' return price level '''
        time = self.event_manager.get_current_time()
        tmp = datetime.fromtimestamp(time)
        fmtdate = int(tmp.strftime("%H"))
        #print tmp, "    ",fmtdate
        if fmtdate >= 9 and fmtdate <=23:
            return HIGH_PRICE
        else:
            return LOW_PRICE     
    get_current_price_level = exposed(get_current_price_level)
        
    
    def monitor_power(self):
        ''' log current system running information including power '''
        time = self.event_manager.get_current_time()
        total_power = self.bqsim.get_running_job_power_usage()
        price = self.get_current_price_level()
        utilization = self.bqsim.get_utilization_rate()
        queue_length = self.bqsim.get_waiting()
          
        ''' accumulate the total energy cost '''
        self.total_cost += total_power * (time - self.event_manager.get_last_schedule_time()) * price
        
        tmp = datetime.fromtimestamp(time)
        fmtdate_hour = int(tmp.strftime("%H"))
        fmtdate_min = int(tmp.strftime("%M"))
        fmtdate_sec = int(tmp.strftime("%S"))
        
        fmttime = 60*fmtdate_hour + fmtdate_min
        
        tag ={}
        tag["unixtime"]= fmttime
        tag["power"] = total_power
        tag["utilization"] = utilization
        
        self.insert_tag(tag)
        
        time_tag = sec_to_date(time)
        if (self.bqsim.is_power_aware()):
            self.log_info1("%s total_power %.5f jobs %d utilization %.5f waiting %d" % (time_tag, total_power, self.bqsim.get_running_job_number(), utilization, queue_length), "power_log_"+self.get_log_title())
        else:
            self.log_info1("%s total_power %.5f jobs %d utilization %.5f waiting %d " % (time_tag, total_power, self.bqsim.get_running_job_number(), utilization, queue_length),"power_log_"+self.get_log_title())

    
    def get_cost(self):
        ''' get total energy cost '''
        return self.total_cost/10000000
    
    
    def log_power_consumption(self):
        ''' log the power information based on the timeline '''
        return self.log_avg_info(self.time_power_list)
    