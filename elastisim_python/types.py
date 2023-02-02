# This file is part of the ElastiSim software.
#
# Copyright (c) 2022, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

from enum import Enum


class JobType(Enum):
    RIGID = 0
    MOLDABLE = 1
    MALLEABLE = 2


class JobState(Enum):
    PENDING = 1
    RUNNING = 4
    PENDING_RECONFIGURATION = 5
    IN_RECONFIGURATION = 6
    COMPLETED = 7
    KILLED = 8


class Job:
    identifier = None
    type = None
    state = None
    walltime = None
    num_nodes = None
    num_gpus_per_node = None
    num_nodes_min = None
    num_nodes_max = None
    num_gpus_per_node_min = None
    num_gpus_per_node_max = None
    submit_time = None
    start_time = None
    end_time = None
    wait_time = None
    makespan = None
    turnaround_time = None
    assigned_nodes = None
    assigned_node_ids = []
    assigned_num_gpus_per_node = None
    arguments = {}
    attributes = {}
    total_phase_count = None
    completed_phases = None
    modified = False
    kill_flag = False

    def __init__(self, job):
        self.identifier = job['id']
        self.type = JobType(job['type'])
        self.state = JobState(job['state'])
        self.walltime = job['walltime']
        if self.type != JobType.RIGID:
            self.num_nodes_min = job['num_nodes_min']
            self.num_nodes_max = job['num_nodes_max']
            self.num_gpus_per_node_min = job['num_gpus_per_node_min']
            self.num_gpus_per_node_max = job['num_gpus_per_node_max']
        else:
            self.num_nodes = job['num_nodes']
            self.num_gpus_per_node = job['num_gpus_per_node']
        self.submit_time = job['submit_time']
        self.start_time = job['start_time']
        self.end_time = job['end_time']
        self.wait_time = job['wait_time']
        self.makespan = job['makespan']
        self.turnaround_time = job['turnaround_time']
        self.assigned_node_ids = set(job['assigned_nodes'])
        self.assigned_num_gpus_per_node = job["assigned_num_gpus_per_node"]
        if 'arguments' in job:
            self.arguments = job['arguments']
        if 'attributes' in job:
            self.attributes = job['attributes']
        self.total_phase_count = job['total_phase_count']
        self.completed_phases = job['completed_phases']

    def __eq__(self, other):
        if isinstance(other, Job):
            return self.identifier == other.identifier
        return False

    def __hash__(self):
        return hash(self.identifier)

    def __lt__(self, other):
        return self.submit_time < other.submit_time

    def assign(self, nodes):
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        if type(nodes) is not list:
            nodes = [nodes]
        self.modified = True
        self.assigned_nodes.extend([node for node in nodes if node.identifier not in self.assigned_node_ids])
        self.assigned_node_ids.update([node.identifier for node in nodes])

    def remove(self, nodes):
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        if type(nodes) is not list:
            nodes = [nodes]
        self.modified = True
        self.assigned_nodes = [node for node in self.assigned_nodes if node not in nodes]
        self.assigned_node_ids.difference_update([node.identifier for node in nodes])

    def kill(self):
        self.modified = True
        self.kill_flag = True

    def assign_num_gpus_per_node(self, assigned_num_gpus_per_node):
        self.assigned_num_gpus_per_node = assigned_num_gpus_per_node

    def to_dict(self):
        return dict(id=self.identifier, assigned_node_ids=[node.identifier for node in self.assigned_nodes],
                    assigned_num_gpus_per_node=self.assigned_num_gpus_per_node, kill_flag=self.kill_flag)


class GpuState(Enum):
    FREE = 0
    ALLOCATED = 1


class Gpu:
    identifier = None
    state = None

    def __init__(self, gpu):
        self.identifier = gpu['id']
        self.state = GpuState(gpu['state'])


class NodeType(Enum):
    COMPUTE_NODE = 0
    COMPUTE_NODE_WITH_BB = 1
    COMPUTE_NODE_WIDE_STRIPED_BB = 2


class NodeState(Enum):
    FREE = 0
    ALLOCATED = 1
    RESERVED = 2


class Node:
    identifier = None
    type = None
    state = None
    assigned_job_ids = None
    gpus = None

    def __init__(self, node):
        self.identifier = node['id']
        self.type = NodeType(node['type'])
        self.state = NodeState(node['state'])
        self.assigned_jobs = None
        self.assigned_job_ids = set(node['assigned_jobs'])
        self.gpus = [Gpu(gpu) for gpu in node['gpus']]
