#!/usr/bin/env python

'''
Created on Dec 28, 2011

@author: zhouzhou
'''

import logging

from Cobalt.Components.qsim_base import PBSlogger
from Cobalt.Proxy import ComponentProxy
from Cobalt.Components.base import Component


logging.basicConfig()
logger = logging.getLogger('powmon')

class PowerMonitor(Component):
    """ Power Monitor. Monitor runtime power consumption, logging into output log or provide to scheduler for power-aware job scheduling"""

    implementation = "powmon"
    name = "powmon"


    def __init__(self, *args, **kwargs):
        Component.__init__(self, *args, **kwargs)
        self.event_manager = ComponentProxy("event-manager")
        self.bqsim = ComponentProxy("queue-manager")
        self.powmon_logger = None
        
    def init_powmon_logger(self):
        if self.iomon_logger == None:
            self.iomon_logger = PBSlogger(self.bqsim.get_outputlog_string() + "-powmon")
        
    
    def monitor_power(self):
        print "monitor_power"
        
        
        