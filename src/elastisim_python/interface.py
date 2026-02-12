# This file is part of the ElastiSim software.
#
# Copyright (c) 2022, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

import zmq
from enum import Enum
from typing import Callable, Any
from zmq import Socket, Context

from elastisim_python import Job, Node


class InvocationType(Enum):
    INVOKE_PERIODIC = 0
    INVOKE_JOB_SUBMIT = 1
    INVOKE_JOB_COMPLETED = 2
    INVOKE_JOB_KILLED = 3
    INVOKE_SCHEDULING_POINT = 4
    INVOKE_EVOLVING_REQUEST = 5
    INVOKE_RECONFIGURATION = 6


class CommunicationCode(Enum):
    ZMQ_INVOKE_SCHEDULING = 0xFFEC4400
    ZMQ_SCHEDULED = 0xFFEC4401
    ZMQ_FINALIZE = 0xFFEC44FF


def link(modified_jobs: list[Job], modified_nodes: list[Node], jobs: list[Job], nodes: list[Node]) -> None:
    for job in modified_jobs:
        job.assigned_nodes = [nodes[node_id] for node_id in job.assigned_node_ids]
    for node in modified_nodes:
        node.assigned_jobs = [jobs[job_id] for job_id in node.assigned_job_ids]


def pass_algorithm(schedule: Callable[[list[Job], list[Node], dict[str, Any]], None], url: str) -> None:
    context: Context = zmq.Context()
    socket: Socket = context.socket(zmq.PAIR)
    socket.connect(url)
    jobs = []
    nodes = []
    while True:
        message = socket.recv_json()
        code = CommunicationCode(message['code'])
        modified_jobs = []
        modified_nodes = []
        if code == CommunicationCode.ZMQ_INVOKE_SCHEDULING:
            for json_job in message['jobs']:
                job = Job(json_job)
                if job.identifier >= len(jobs):
                    jobs.append(job)
                else:
                    jobs[job.identifier] = job
                modified_jobs.append(job)
            for json_node in message['nodes']:
                node = Node(json_node)
                if node.identifier >= len(nodes):
                    nodes.append(node)
                else:
                    nodes[node.identifier] = node
                modified_nodes.append(node)
            link(modified_jobs, modified_nodes, jobs, nodes)
            system = {}
            for key, _ in message.items():
                if key == 'invocation_type':
                    invocation_type = InvocationType(message['invocation_type'])
                    system['invocation_type'] = invocation_type
                    if invocation_type != InvocationType.INVOKE_PERIODIC:
                        system['job'] = jobs[message['job_id']]
                        if invocation_type == InvocationType.INVOKE_EVOLVING_REQUEST:
                            system['evolving_request'] = int(message['evolving_request'])
                else:
                    system[key] = message[key]
            schedule(jobs, nodes, system)
            message = dict(code=CommunicationCode.ZMQ_SCHEDULED.value,
                           jobs=[job.to_dict() for job in jobs if job.modified])
            socket.send_json(message)
        elif code == CommunicationCode.ZMQ_FINALIZE:
            break
    socket.close()
