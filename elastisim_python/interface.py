# This file is part of the ElastiSim software.
#
# Copyright (c) 2022, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

import zmq
from enum import Enum

from elastisim_python import Job, Node


class InvocationType(Enum):
    INVOKE_PERIODIC = 0
    INVOKE_JOB_SUBMIT = 1
    INVOKE_JOB_COMPLETED = 2
    INVOKE_JOB_KILLED = 3
    INVOKE_SCHEDULING_POINT = 4


class CommunicationCode(Enum):
    ZMQ_INVOKE_SCHEDULING = 0xFFEC4400
    ZMQ_SCHEDULED = 0xFFEC4401
    ZMQ_FINALIZE = 0xFFEC44FF


def link(jobs, nodes):
    for job in jobs:
        job.assigned_nodes = [nodes[node_id] for node_id in job.assigned_node_ids]
    for node in nodes:
        node.assigned_jobs = [jobs[job_id] for job_id in node.assigned_job_ids]


def pass_algorithm(schedule, url):
    context = zmq.Context()
    socket = context.socket(zmq.PAIR)
    socket.connect(url)
    while True:
        message = socket.recv_json()
        code = CommunicationCode(message['code'])
        if code == CommunicationCode.ZMQ_INVOKE_SCHEDULING:
            jobs = []
            for job in message['jobs']:
                jobs.append(Job(job))
            nodes = []
            for node in message['nodes']:
                nodes.append(Node(node))
            link(jobs, nodes)
            system = {}
            system['time'] = message['time']
            system['invocation_type'] = InvocationType(message['invocation_type'])
            job = None
            if system['invocation_type'] != InvocationType.INVOKE_PERIODIC:
                job = jobs[message['job_id']]
            system['pfs_read_bw'] = message['pfs_read_bw']
            system['pfs_write_bw'] = message['pfs_write_bw']
            system['pfs_read_utilization'] = message['pfs_read_utilization']
            system['pfs_write_utilization'] = message['pfs_write_utilization']
            schedule(jobs, nodes, system, job)
            message = dict(code=CommunicationCode.ZMQ_SCHEDULED.value,
                           jobs=[job.to_dict() for job in jobs if job.modified])
            socket.send_json(message)
        elif code == CommunicationCode.ZMQ_FINALIZE:
            break
    socket.close()
