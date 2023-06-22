# This file is part of the ElastiSim software.
#
# Copyright (c) 2023, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

from __future__ import annotations
from enum import Enum
from typing import Any


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
    identifier: int = None
    type: JobType = None
    state: JobState = None
    walltime: float = None
    num_nodes: int = None
    num_gpus_per_node: int = None
    num_nodes_min: int = None
    num_nodes_max: int = None
    num_gpus_per_node_min: int = None
    num_gpus_per_node_max: int = None
    submit_time: float = None
    start_time: float = None
    end_time: float = None
    wait_time: float = None
    makespan: float = None
    turnaround_time: float = None
    assigned_nodes: list[Node] = None
    assigned_node_ids: set[int] = []
    assigned_num_gpus_per_node: int = None
    arguments: dict[str, str] = {}
    attributes: dict[str, str] = {}
    total_phase_count: int = None
    completed_phases: int = None
    modified: bool = False
    kill_flag: bool = False

    def __init__(self, job: dict[str, Any]) -> None:
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

    def __eq__(self, other: Job) -> bool:
        if isinstance(other, Job):
            return self.identifier == other.identifier
        return False

    def __hash__(self) -> int:
        return hash(self.identifier)

    def __lt__(self, other: Job) -> bool:
        return self.identifier < other.identifier

    def assign(self, nodes: Node | list[Node]) -> None:
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        if type(nodes) is not list:
            nodes = [nodes]
        self.modified = True
        self.assigned_nodes.extend([node for node in nodes if node.identifier not in self.assigned_node_ids])
        self.assigned_node_ids.update([node.identifier for node in nodes])

    def remove(self, nodes: Node | list[Node]) -> None:
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        if type(nodes) is not list:
            nodes = [nodes]
        self.modified = True
        self.assigned_nodes = [node for node in self.assigned_nodes if node not in nodes]
        self.assigned_node_ids.difference_update([node.identifier for node in nodes])

    def kill(self) -> None:
        self.modified = True
        self.kill_flag = True

    def assign_num_gpus_per_node(self, assigned_num_gpus_per_node: int) -> None:
        self.assigned_num_gpus_per_node = assigned_num_gpus_per_node

    def to_dict(self) -> dict[str, Union[int, list[int], bool]]:
        return dict(id=self.identifier, assigned_node_ids=[node.identifier for node in self.assigned_nodes],
                    assigned_num_gpus_per_node=self.assigned_num_gpus_per_node, kill_flag=self.kill_flag)
