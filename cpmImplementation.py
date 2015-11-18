#!/usr/bin/env python
"""
2013.3.12 CKS

A simple critical path method implementation.

http://en.wikipedia.org/wiki/Critical_path_method
"""
import sys
import unittest
import random

VERSION = (0, 1, 1)
__version__ = '.'.join(map(str, VERSION))

class Node(object):
    """
    Represents a task in a action precedence network.
    
    Nodes can be linked together or group child nodes.
    """
    
    def __init__(self, idNo, name, duration=None, lag=0):
        
        self.parent = None
        self.idNo = idNo
        
        # A unique identifier of this task.
        self.name = name
        
        self.description = None
        
        # How long this task takes to complete.
        self._duration = duration
        
        # The amount of time the task must wait after the preceeding task
        # has finished before beginning.
        self._lag = lag #TODO
        
        self.drag = None #TODO
        
        # Earliest start time.
        self._es = None
        
        # Earliest finish time.
        self._ef = None
        
        # Latest start time.
        self._ls = None
        
        # Latest finish time.
        self._lf = None
        
        # The amount time that the activity can be delayed without
        # changing the start of any other activity.
        self._free_float = None #TODO
        
        # The amount of time that the activity can be delayed without
        # increasing the overall project's duration.
        self._total_float = None #TODO
        
        self.nodes = []#set()
        self.name_to_node = {}
        self.to_nodes = set()
        self.incoming_nodes = set()
        
        self.forward_pending = set()
        self.backward_pending = []
        
        self._critical_path = None
        
        self.exit_node = None

        # List of workers assigned to task at any given time
        self.workers = []
        self.isActive = False
    
    def lookup_node(self, name):
        return self.name_to_node[name]
    
    @property
    def lag(self):
        return self._lag
    
    @lag.setter
    def lag(self, v):
        self._lag = v
    
    @property
    def duration(self):
        return self._duration
    
    @duration.setter
    def duration(self, v):
        """
        This should only be set by update_all() after calculating
        the critical path of all child nodes.
        """
        self._duration = v
        
    @property
    def es(self):
        return self._es
    
    @es.setter
    def es(self, v):
        self._es = v
        if self.parent:
            self.parent.forward_pending.add(self)
        
    @property
    def ef(self):
        return self._ef
    
    @ef.setter
    def ef(self, v):
        self._ef = v
    
    @property
    def ls(self):
        return self._ls
    
    @ls.setter
    def ls(self, v):
        self._ls = v
    
    @property
    def lf(self):
        return self._lf
    
    @lf.setter
    def lf(self, v):
        self._lf = v
    
    @property
    def total_float(self):
        return self._total_float
    
    @total_float.setter
    def total_float(self, v):
        self._total_float = v
    
    def __repr__(self):
        return str(self.name)
        
    def __hash__(self):
        return hash(self.name)
        
    def __cmp__(self, other):
        if not isinstance(other, type(self)):
            return NotImplmented
        return cmp(self.name, other.name)
    
    def add(self, node):
        """
        Includes the given node as a child node.
        """
        assert isinstance(node, Node), \
            'Only Node instances can be added, not %s.' % (type(node).__name__,)
        assert node.duration is not None, 'Duration must be specified.'
        #self.nodes.add(node)
        self.nodes.append(node)
        self.name_to_node[node.name] = node
        node.parent = self
        self.forward_pending.add(node)
        self._critical_path = None
        return node
    
    def link(self, from_node, to_node=None):
        """
        Links together two child nodes.
        """
        #print 'from_node:',from_node
        if not isinstance(from_node, Node):
            from_node = self.name_to_node[from_node]
        #print 'from_node:',from_node
        assert isinstance(from_node, Node)
        if to_node is not None:
            if not isinstance(to_node, Node):
                to_node = self.name_to_node[to_node]
            assert isinstance(to_node, Node)
            from_node.to_nodes.add(to_node)
            to_node.incoming_nodes.add(from_node)
        else:
            self.to_nodes.add(from_node)
            from_node.incoming_nodes.add(self)
        return self

    @property
    def first_nodes(self):
        """
        Returns all child nodes that have no in-bound dependencies.
        """
        first = set(self.nodes)
        for node in self.nodes:
            first.difference_update(node.to_nodes)
        return first

    @property
    def last_nodes(self):
        """
        Returns all child nodes that have to out-bound dependencies.
        """
        return [_ for _ in self.nodes if not _.to_nodes]

    def update_forward(self):
        """
        Updates forward timing calculations for the current node.
        
        Assumes the earliest start value has already been set.
        """
        changed = False
#        print 'updating forward:',self.name
        if self.es is not None and self.duration is not None:
#            print 'es:',self.es
#            print 'dur:',self.duration
            self.ef = self.es + self.duration
            changed = True
        
        if changed:
            for to_node in self.to_nodes:
                if to_node == self:
                    continue
                # Earliest start of the succeeding activity is the earliest finish
                # of the preceding activity plus possible lag.
                new_es = self.ef + to_node.lag
                if to_node.es is None:
                    to_node.es = new_es
                else:
                    to_node.es = max(to_node.es, new_es)
                    
                if self.parent:
                    self.parent.forward_pending.add(to_node)
            
            if self.parent:
                self.parent.backward_pending.append(self)

    def update_backward(self):
        """
        Updates backward timing calculations for the current node.
        """
#        print 'update_backward0:',self.name,self.ls,self.lf
#        print '\tto_nodes:',[_.ls for _ in self.to_nodes]
        if self.lf is None:
            if self.to_nodes:
                #print min([_.ls for _ in self.to_nodes], 1e99999)
                self.lf = min([_.ls for _ in self.to_nodes])
            else:
                self.lf = self.ef
            #assert self.lf is not None, 'No latest finish time could be found.' #TODO
        self.ls = self.lf - self.duration
        self.total_float = self.ls - self.es
        #self.ls = (self.lf or 0) - self.duration
#        print 'update_backward1:',self.name,self.ls,self.lf

    def add_exit(self):
        """
        Links all leaf nodes to a common exit node.
        """
        if self.exit_node is None:
            self.exit_node = Node('EXIT', duration=0)
            self.add(self.exit_node)
        for node in self.nodes:
            if node is self.exit_node:
                continue
            if not node.to_nodes:
                self.link(from_node=node, to_node=self.exit_node)

    def update_all(self):
        """
        Updates timing calculations for all children nodes.
        """
        assert self.is_acyclic(), 'Network must not contain any cycles.'
        
        for node in list(self.forward_pending.intersection(self.first_nodes)):
            node.es = self.lag + node.lag
            node.update_forward()
            self.forward_pending.remove(node)
        
        i = 0
        forward_priors = set()
        while self.forward_pending:
            i += 1
#            print '\rCalculating forward paths %i...' % (i,),
#            sys.stdout.flush()
            q = set(self.forward_pending)
            self.forward_pending.clear()
            while q:
                node = q.pop()
                if node in forward_priors:
                    continue
                #forward_priors.add(node)
                node.update_forward()
#        print
        
        i = 0
        backward_priors = set()
        while self.backward_pending:
            i += 1
#            print '\rCalculating backward paths %i...' % (i,),
#            sys.stdout.flush()
            node = self.backward_pending.pop()
            if node in backward_priors:
                continue
            #backward_priors.add(node)
            node.update_backward()
#        print
            
        self._critical_path = duration, path, priors = self.get_critical_path(as_item=True)
        self.duration = duration
        self.es = path[0].es
        self.ls = path[0].ls
        self.ef = path[-1].ef
        self.lf = path[-1].lf
#            
    def get_critical_path(self, as_item=False):
        """
        Finds the longest path in among the child nodes.
        """
        if self._critical_path is not None:
            # Returned cached path.
            return self._critical_path[1]
        longest = None
        q = [(_.duration, [_], set([_])) for _ in self.first_nodes]
        while q:
            item = length, path, priors = q.pop(0)
            if longest is None:
                longest = item
            else:
                longest = max(longest, item)
            for to_node in path[-1].to_nodes:
                if to_node in priors:
                    continue
                q.append((length+to_node.duration, path+[to_node], priors.union([to_node])))
        if longest is None:
            return
        elif as_item:
            return longest
        else:
            return longest[1]

    def print_times(self):
        w = 7
        print """
+{border}+
|{blank} DUR={dur} {blank}|
+{border}+
|ES={es}|{blank}|EF={ef}|
|{segment}|{name}|{segment}|
|LS={ls}|{blank}|LF={lf}|
+{border}+
|{blank}DRAG={drag}{blank}|
+{border}+
""".format(**dict(
            blank=' '*w,
            segment='-'*w,
            border='-'*(w*3 + 2),
            dur=str(self.duration).ljust(w-4),
            es=str(self.es).ljust(w-3),
            ef=str(self.ef).ljust(w-3),
            name=str(self.name).center(w),
            ls=str(self.ls).ljust(w-3),
            lf=str(self.lf).ljust(w-3),
            drag=str(self.drag).ljust(w-5),
        ))

    def is_acyclic(self):
        """
        Returns true if the network has no cycle anywhere within it
        by performing a depth-first search of all nodes.
        Returns false otherwise.
        A proper task network should be acyclic, having an explicit
        "start" and "end" node with no link back from end to start.
        """
        q = [(_, set([_])) for _ in self.nodes]
        while q:
            node, priors = q.pop(0)
            for next_node in node.to_nodes:
                if next_node in priors:
                    return False
                next_priors = priors.copy()
                next_priors.add(next_node)
                q.append((next_node, next_priors))
        return True

class Test(unittest.TestCase):
    
    def test_cycles(self):
        
        p = Node('project')
        
        a = p.add(Node('A', duration=3))
        b = p.add(Node('B', duration=3, lag=0))
        c = p.add(Node('C', duration=4, lag=0))
        d = p.add(Node('D', duration=6, lag=0))
        e = p.add(Node('E', duration=5, lag=0))
        
        p.link(a, b)
        p.link(a, c)
        p.link(a, d)
        p.link(b, e)
        p.link(c, e)
        p.link(d, e)
        
        self.assertEqual(p.is_acyclic(), True)
        
        p = Node('project')
        
        a = p.add(Node('A', duration=3))
        b = p.add(Node('B', duration=3, lag=0))
        c = p.add(Node('C', duration=4, lag=0))
        d = p.add(Node('D', duration=6, lag=0))
        e = p.add(Node('E', duration=5, lag=0))
        
        p.link(a, b)
        p.link(a, c)
        p.link(a, d)
        p.link(b, e)
        p.link(c, e)
        p.link(d, e)
        p.link(e, a) # links back!
        
        self.assertEqual(p.is_acyclic(), False)
    
    def test_project(self):
        
        p = Node('project')
        
        a = p.add(Node('A', duration=3))
        b = p.add(Node('B', duration=3, lag=0))
        c = p.add(Node('C', duration=4, lag=0))
        d = p.add(Node('D', duration=6, lag=0))
        e = p.add(Node('E', duration=5, lag=0))
        
        p.link(a, b)
        p.link(a, c)
        p.link(a, d)
        p.link(b, e)
        p.link(c, e)
        p.link(d, e)
        
        p.update_all()
        
#        for node in sorted(p.nodes, key=lambda n: n.name):
#            node.print_times()
            
        self.assertEqual(a.es, 0)
        self.assertEqual(a.ef, 3)
        self.assertEqual(a.ls, 0)
        self.assertEqual(a.lf, 3)
        self.assertEqual(b.es, 3)
        self.assertEqual(b.ef, 6)
        self.assertEqual(b.ls, 6)
        self.assertEqual(b.lf, 9)
        self.assertEqual(c.es, 3)
        self.assertEqual(c.ef, 7)
        self.assertEqual(c.ls, 5)
        self.assertEqual(c.lf, 9)
        self.assertEqual(d.es, 3)
        self.assertEqual(d.ef, 9)
        self.assertEqual(d.ls, 3)
        self.assertEqual(d.lf, 9)
        self.assertEqual(e.es, 9)
        self.assertEqual(e.ef, 14)
        self.assertEqual(e.ls, 9)
        self.assertEqual(e.lf, 14)
        
        critical_path = p.get_critical_path()
        #print critical_path
        self.assertEqual(critical_path, [a, d, e])
        self.assertEqual(p.duration, 14)
        self.assertEqual(p.es, 0)
        self.assertEqual(p.ef, 14)
        self.assertEqual(p.ls, 0)
        self.assertEqual(p.lf, 14)

def printGantt(p):
    daysOfTheWeek = ["M", "T", "W", "R", "F"]
    #sort project nodes by early start date 
    p.nodes.sort(key=lambda x: x.es)
    
    print str(p.name) + " Gantt Chart: "
    for x in range(0, p.duration):
        print daysOfTheWeek[x%5],
    print "\n"
    for node in p.nodes:
        for x in range(0, p.duration):
            if x == node.es:
                print node.name,
            elif x >= node.es and x < node.es + node.duration:
                print node.name,
            elif x >= node.es + node.duration and x < node.es + node.duration + node.total_float:
                print "~",
            else:
                print " ",
        print "\n"

def printProjectOverview(p):
    print str(p.name) + " Overview: "
    print "Task\tDur\tES\tEF\tLS\tLF\tFloat\tPrereq"
    for node in p.nodes:
        print str(node.name) + "\t" + str(node.duration) + "\t" + str(node.es) + "\t" + str(node.ef) + "\t" + str(node.ls) + "\t" + str(node.lf) + "\t" + str(node._total_float) + "\t" + str(node.incoming_nodes)
    print "Critical path: " + str(p.get_critical_path())
    print "Minimum duration: " + str(p.duration)
    print "\n"

class ProjectAssignment:
    def __init__(self, name):
        self.name = name
        self.workers = []
        
    def setWorkers(self, workers):
        self.workers = workers

class Worker:
    def __init__(self, name):
        self.name = name
        self.efficiencyRatings = []
        self.isAvailable = True

    def setEfficiencyRatings(self, ratings):
        self.efficiencyRatings = ratings

def getActiveTasks(taskList, currentDay):
    activeTasks = []
    for task in taskList:
        if task.es <= currentDay and currentDay < task.es + task.duration:
            activeTasks.append(task)
    return activeTasks

def checkIfResourcesUnused(resources):
    for r in resources:
        if r.isAvailable == True:
            return True
    return False

def initializeProjectScheduleDataStructure(project):
    projectSchedule = []
    for day in range(0, project.duration):
        projectSchedule.append([])
        for task in project.nodes:
            projectSchedule[day].append([])
    return projectSchedule

def printProjectScheduleDataStructure(project, pds):
    for day in range(0, len(pds)):
        print "Day: " + str(day + 1)
        for task in project.nodes:
            if pds[day][project.nodes.index(task)] != []:
                print "Task " + str(task.name) + " - ",
                for worker in pds[day][project.nodes.index(task)]:
                    print worker.name,
                print ""        
        print "-------------------------"
                

def assignResourcesRandomly(resources, project, scheduleDataStructure):
    for day in range(0, project.duration):
        activeTasks = getActiveTasks(project.nodes, day)
        for task in activeTasks:
            taskFilled = False
            while taskFilled == False:
                resource = random.choice(resources)
                if resource.isAvailable:
                    scheduleDataStructure[day][project.nodes.index(task)].append(resource)
                    resource.isAvailable = False
                    taskFilled = True
        if checkIfResourcesUnused(resources) == True:
            for r in resources:
                if r.isAvailable == True:
                    task = random.choice(activeTasks)
                    scheduleDataStructure[day][project.nodes.index(task)].append(r)
        for resource in resources:
            resource.isAvailable = True
            
    return scheduleDataStructure

def scoreResourceAssignment(project, scheduleDataStructure):
    totalScore = 0.0
    dayScore = 0.0
    taskScore = 0.0
    print "Scoring: "
    for day in range(0, len(scheduleDataStructure)):
        print "Day: " + str(day + 1)
        for task in project.nodes:
            if scheduleDataStructure[day][project.nodes.index(task)] != []:
                print "Task " + str(task.name) + " - ",
                for worker in scheduleDataStructure[day][project.nodes.index(task)]:
                    taskScore += worker.efficiencyRatings[task.idNo - 1]
                if task.total_float > 0:
                    taskScore /= task.total_float
                elif task.total_float == 0:
                    taskScore *= task.duration
                print "Task score: " + str(taskScore)
                dayScore += taskScore
                taskScore = 0.0
        print "Day score: " + str(dayScore)
        totalScore += dayScore
        dayScore = 0.0
    print "Project score: " + str(totalScore)
           
        
def main():
    p = Node(0, 'MyProject')
    a = p.add(Node(1, 'A', duration=4))
    b = p.add(Node(2, 'B', duration=5))
    c = p.add(Node(3, 'C', duration=3))
    d = p.add(Node(4, 'D', duration=3))
    e = p.add(Node(5, 'E', duration=9))
    f = p.add(Node(6, 'F', duration=6))
    g = p.add(Node(7, 'G', duration=2))
    h = p.add(Node(8, 'H', duration=2))

    p.link(a,b).link(b,c).link(c,h).link(d,e).link(d,f).link(e,g).link(f,g).link(g,h)
    p.update_all()

    printProjectOverview(p)
    printGantt(p)

    # Establish resource pool
    steve = Worker("Steve")
    bob = Worker("Bob")
    jim = Worker("Jim")
    
    # Set efficiency ratings matrix for each resource and task
    steve.setEfficiencyRatings([5,4,3,2,1,2,2,4])
    bob.setEfficiencyRatings([1,4,4,5,2,4,3,2])
    jim.setEfficiencyRatings([3,4,5,1,1,4,2,5])

    # Assign resources randomly to tasks
    scheduleDataStructure = initializeProjectScheduleDataStructure(p)
    print "Randomly assigning resources to tasks..."
    scheduleDataStructure = assignResourcesRandomly([steve, bob, jim], p, scheduleDataStructure)
    printProjectScheduleDataStructure(p, scheduleDataStructure)
    scoreResourceAssignment(p, scheduleDataStructure)
    
    
if __name__ == '__main__':
    main()
    
