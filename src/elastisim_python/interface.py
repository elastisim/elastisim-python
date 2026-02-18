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
import msgpack

from .job import Job
from .node import Node


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

def pass_algorithm(schedule: Callable[[list[Job], list[Node], dict[str, Any]], None], url: str) -> None:
    context: Context = zmq.Context()
    socket: Socket = context.socket(zmq.PAIR)
    socket.connect(url)
    jobs: list[Job] = []
    nodes: list[Node] = []
    while True:
        message: dict[str, Any] = msgpack.unpackb(socket.recv())
        code = CommunicationCode(message['code'])
        if code == CommunicationCode.ZMQ_INVOKE_SCHEDULING:
            for json_job in message['jobs']:
                identifier: int = json_job['id']
                if identifier >= len(jobs):
                    job = Job(json_job)
                    jobs.append(job)
                else:
                    job = jobs[identifier]
                    job.update(json_job, nodes)
            for json_node in message['nodes']:
                identifier: int = json_node['id']
                if identifier >= len(nodes):
                    node = Node(json_node)
                    nodes.append(node)
                else:
                    node = nodes[identifier]
                    node.update(json_node, jobs)
            system = dict(message)
            invocation_type = InvocationType(message['invocation_type'])
            system['invocation_type'] = invocation_type
            if invocation_type != InvocationType.INVOKE_PERIODIC:
                system['job'] = jobs[message['job_id']]
                if invocation_type == InvocationType.INVOKE_EVOLVING_REQUEST:
                    system['evolving_request'] = int(message['evolving_request'])
            schedule(jobs, nodes, system)
            message = dict(code=CommunicationCode.ZMQ_SCHEDULED.value,
                           jobs=[job.to_dict() for job in jobs if job.modified])
            socket.send(msgpack.packb(message))
        elif code == CommunicationCode.ZMQ_FINALIZE:
            break
        else:
            raise ValueError(
                f'Received unknown code {code} from simulation engine')
    socket.close()
