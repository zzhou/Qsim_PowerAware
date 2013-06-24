#!/usr/bin/env python

'''
Created on 2012-9-21

@author: zhouzhou
'''
import sys
from Cobalt.Proxy import ComponentProxy

# Sorting order #
DESC = 0
ASC = 1
COMP_SYSTEM = "system"

# compare function
# order the jobs with the lowest power consumption first
def powercmp_asc(job1, job2):
    power1 = get_power(job1)
    power2 = get_power(job2)
    return cmp(power1, power2)

#order the jobs with the most power consumption first
def powercmp_desc(job1, job2):
    power1 = get_power(job1)
    power2 = get_power(job2)
    return -cmp(power1, power2)

#order the jobs with the most nodes first
def nodescmp_desc(job1, job2):
    return -cmp(job1.nodes, job2.nodes)

# Compare function for 0-1 Knapsack 
def select_max(power1, power2):
    if power1 >= power2:
        # not selected
        return (power1, -1)
    else:
        # selected
        return (power2, 1)
    
def select_min(power1, power2):
    if power1 <= power2:
        # not selected
        return (power1, -1)
    else:
        # selected
        return (power2, 1)

''' get_power '''
def get_power(job):
    node = int(job.nodes)
    power = int(job.power)
    rack = float(node) / 1024
    return float(power*rack) 

def get_unit_power(job):
    return job.power

''' get weight'''
def get_weight(job):
    return get_power(job)

''' get midplane count '''
def get_mid(job):
    nodes = int(job.nodes)
    
    if (nodes % 512) == 0:
        return int(nodes / 512)
    else:
        return int(nodes / 512) + 1

''' get value'''
def get_value(job, partition_dict):
    partition_name = partition_dict[str(job.jobid)]
    
    partition_size = ComponentProxy(COMP_SYSTEM).get_partition_size(partition_name)
    return partition_size

def get_value2(job):
    node = int(job.nodes)
    rest_node = node % 512
    
    if rest_node == 0:
        return int(node/512)
    else:
        return int(node/512)+1
    

''' find optimal solution using DP '''
def find_optimal_solu_DP(power_budget, partition_dict, all_jobs):
    in_list ={}
    ex_list ={}
    length = len(partition_dict)   
    
    if length > 0:
        if power_budget <= 0:
#            print "No optimal solution because no power budget"
            return in_list, partition_dict
        
        weight = range(0, length+1)
        value = range(0, length+1)
        jobs = range(0, length+1)
        
        val = [[0 for x in range(power_budget+1)] for y in range(length+1)]
        solu = [[0 for x in range(power_budget+1)] for y in range(length+1)]
                     
        # initialization
        for i in range(0, power_budget+1):
            val[0][i] = 0
        
        for i in range(0, length+1):
            val[i][0] = 0
            
        i = 1    
        for jobid in partition_dict:
            job = all_jobs[int(jobid)]
            weight[i] = int(get_weight(job))
            value[i] = int(get_value(job, partition_dict))
            jobs[i] = job
            i += 1
        
        # search using dynamic programming
        for i in range(1, length+1):
            for pb in range(power_budget, -1, -1):
                if weight[i] > pb:
                    val[i][pb] = val[i-1][pb]
                    solu[i][pb] = -1                              
                else:
                    val[i][pb],solu[i][pb] = select_max(val[i-1][pb], val[i-1][pb-weight[i]]+value[i])                             
    
#                    print self.get_current_time(), "optimal total value: ", val[length][power_budget]
        # print solution
        pb = power_budget
        for i in range(length, 0, -1):
            if solu[i][pb] == 1:
#                            print "solu[%d][%d]:%d %s selected, size: %d, power: %d" % (i, pb, solu[i][pb], jobs[i].jobid, int(get_value(jobs[i])), int(get_weight(jobs[i])))
                in_list.update({str(jobs[i].jobid):partition_dict[str(jobs[i].jobid)]})
                pb -= weight[i]
            elif solu[i][pb] == -1:
#                            print "solu[%d][%d]:%d %s not selected because overweight, size:%d, power: %d" % (i, pb, solu[i][pb], jobs[i].jobid, int(get_value(jobs[i])), int(get_weight(jobs[i])))
                ex_list.update({str(jobs[i].jobid):partition_dict[str(jobs[i].jobid)]})
            elif solu[i][pb] == -2:
#                            print "solu[%d][%d]:%d %s not selected because less, size:%d, power: %d" % (i, pb, solu[i][pb], jobs[i].jobid, int(get_value(jobs[i])), int(get_weight(jobs[i])))
                ex_list.update({str(jobs[i].jobid):partition_dict[str(jobs[i].jobid)]})
    
    return in_list, ex_list


''' weight: nodes, value: power; minimize power while not fill as many jobs as possible '''
def find_optimal_solu_DP_min_power_BGP(power_budget, partition_dict, all_jobs):
    in_list ={}
    ex_list ={}
    length = len(partition_dict)   
    
    if length > 0:
        weight = range(0, length+1)
        value = range(0, length+1)
        jobs = range(0, length+1)
        
        val = [[0 for x in range(power_budget+1)] for y in range(length+1)]
        solu = [[0 for x in range(power_budget+1)] for y in range(length+1)]
                        
        # initialization
        for i in range(0, power_budget+1):
            val[0][i] = sys.maxint
        
        for i in range(0, length+1):
            val[i][0] = 0
            
        val[0][0] = 0
            
        i = 1    
        for jobid in partition_dict:
            job = all_jobs[int(jobid)]
            weight[i] = int(get_weight(job))
            value[i] = int(get_value(job, partition_dict))
            jobs[i] = job
            i += 1
        
        # search using dynamic programming
        for i in range(1, length+1):
            for pb in range(power_budget, -1, -1):
                if weight[i] > pb:
                    val[i][pb] = val[i-1][pb]
                    solu[i][pb] = -1                              
                else:
                    val[i][pb],solu[i][pb] = select_min(val[i-1][pb], val[i-1][pb-weight[i]]+value[i])                             

        # print solution
        pb = power_budget
        for i in range(length, 0, -1):
            if solu[i][pb] == 1:
#                print "solu[%d][%d]:%d %s selected, size: %d, power: %d" % (i, pb, solu[i][pb], jobs[i].jobid, int(get_value(jobs[i])), int(get_weight(jobs[i])))
                in_list.update({str(jobs[i].jobid):partition_dict[str(jobs[i].jobid)]})
                pb -= weight[i]
            elif solu[i][pb] == -1:
#                print "solu[%d][%d]:%d %s not selected because overweight, size:%d, power: %d" % (i, pb, solu[i][pb], jobs[i].jobid, int(get_value(jobs[i])), int(get_weight(jobs[i])))
                ex_list.update({str(jobs[i].jobid):partition_dict[str(jobs[i].jobid)]})
    
    return in_list, ex_list


def find_optimal_solu_Naive(power_budget, active_jobs):
    in_list = []
    ex_list = []
    
    for job in active_jobs:
        power = get_power(job)
        if power < power_budget:
            in_list.append(job)
            power_budget = power_budget - power
        else:
            ex_list.append(job)
        
    return in_list, ex_list


def find_optimal_solu_Greedy(power_budget, active_jobs, sort_pattern):
    ex_list = []

#    if sort_pattern == ASC:
#        ''' asc '''
#        active_jobs.sort(powercmp_asc)
#    else:
#        ''' desc '''
#        active_jobs.sort(powercmp_desc)

    active_jobs.sort(nodescmp_desc)
    return active_jobs, ex_list
    
    
''' minimize power  ''' 
def find_optimal_solu_DP_min_power(idle_nodes, active_jobs):
    in_list = []
    ex_list = []
    
    length = len(active_jobs)
    
    if length > 0:
        if idle_nodes <= 0:
            return in_list, active_jobs
        
        weight = range(0, length+1)
        value = range(0, length+1)
        jobs = range(0, length+1)
        
        val = [[0 for x in range(idle_nodes+1)] for y in range(length+1)]
        solu = [[0 for x in range(idle_nodes+1)] for y in range(length+1)]
                     
        # initialization
        for i in range(0, idle_nodes+1):
            val[0][i] = sys.maxint
        
        for i in range(0, length+1):
            val[i][0] = 0
            
        val[0][0] = 0
            
        i = 1    
        for job in active_jobs:
            weight[i] = int(get_weight(job))
            value[i] = int(get_value2(job))
            jobs[i] = job
            i += 1
        
        # search using dynamic programming
        for i in range(1, length+1):
            for inode in range(idle_nodes, -1, -1):
                if weight[i] > inode:
                    val[i][inode] = val[i-1][inode]
                    solu[i][inode] = -1                              
                else:
                    val[i][inode],solu[i][inode] = select_min(val[i-1][inode], val[i-1][inode-weight[i]]+value[i])                             
    
#       print self.get_current_time(), "optimal total value: ", val[length][power_budget]
        # print solution
        inode = idle_nodes
        for m in range(inode, 0, -1):
            if val[length][m] < sys.maxint:
                for i in range(length, 0, -1):
                    if solu[i][m] == 1:
                        in_list.append(jobs[i])
                        inode -= weight[i]
                    elif solu[i][m] == -1:
                        ex_list.append(jobs[i])
                break
    return in_list, ex_list


''' maximize power  ''' 
def find_optimal_solu_DP_max_power(idle_nodes, active_jobs):
    in_list = []
    ex_list = []
    
    length = len(active_jobs)
    
    if length > 0:
        if idle_nodes <= 0:
#                        print "No optimal solution because no power budget"
            return in_list, active_jobs
        
        weight = range(0, length+1)
        value = range(0, length+1)
        jobs = range(0, length+1)
        
        val = [[0 for x in range(idle_nodes+1)] for y in range(length+1)]
        solu = [[0 for x in range(idle_nodes+1)] for y in range(length+1)]
                     
        # initialization
        for i in range(0, idle_nodes+1):
            val[0][i] = 0
        
        for i in range(0, length+1):
            val[i][0] = 0
            
        val[0][0] = 0
            
        i = 1    
        for job in active_jobs:
            weight[i] = int(get_weight(job))
            value[i] = int(get_value2(job))
            jobs[i] = job
            i += 1
        
        # search using dynamic programming
        for i in range(1, length+1):
            for inode in range(idle_nodes, -1, -1):
                if weight[i] > inode:
                    val[i][inode] = val[i-1][inode]
                    solu[i][inode] = -1                              
                else:
                    val[i][inode],solu[i][inode] = select_max(val[i-1][inode], val[i-1][inode-weight[i]]+value[i])                             
    
#       print self.get_current_time(), "optimal total value: ", val[length][power_budget]
        # print solution
        inode = idle_nodes
        for i in range(length, 0, -1):
            if solu[i][inode] == 1:
                in_list.append(jobs[i])
                inode -= weight[i]
            elif solu[i][inode] == -1:
                ex_list.append(jobs[i])
    return in_list, ex_list
    
    