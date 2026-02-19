# This file is part of the ElastiSim software.
#
# Copyright (c) 2023, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

from __future__ import annotations

from enum import Enum
from typing import Any, TYPE_CHECKING


if TYPE_CHECKING:
    from .node import Node


class JobType(Enum):
    RIGID = 0
    MOLDABLE = 1
    MALLEABLE = 2
    EVOLVING = 3
    ADAPTIVE = 4


class JobState(Enum):
    PENDING = 1
    RUNNING = 4
    PENDING_RECONFIGURATION = 5
    IN_RECONFIGURATION = 6
    COMPLETED = 7
    KILLED = 8


class Job:
    def __init__(self, json_job: dict[str, Any]) -> None:
        self.identifier: int = json_job['id']
        self.type: JobType = JobType(json_job['type'])
        self.submit_time: float = json_job['submit_time']
        self.walltime: float = json_job['walltime']
        if self.type != JobType.RIGID:
            self.num_nodes_min: int = json_job['num_nodes_min']
            self.num_nodes_max: int = json_job['num_nodes_max']
            self.num_gpus_per_node_min: int = json_job['num_gpus_per_node_min']
            self.num_gpus_per_node_max: int = json_job['num_gpus_per_node_max']
        else:
            self.num_nodes: int = json_job['num_nodes']
            self.num_gpus_per_node: int = json_job['num_gpus_per_node']
        self.arguments: dict[str, str] = json_job.get('arguments', {})
        self.attributes: dict[str, str] = json_job.get('attributes', {})
        self.modified: bool = False
        self.modified_runtime_args: bool = False
        self.kill_flag: bool = False
        self._load(json_job)

    def __eq__(self, other) -> bool:
        if isinstance(other, Job):
            return self.identifier == other.identifier
        return False

    def __hash__(self) -> int:
        return hash(self.identifier)

    def __lt__(self, other: Job) -> bool:
        return self.identifier < other.identifier

    def __repr__(self) -> str:
        return (
            f'Job(id={self.identifier}, type={self.type.name}, '
            f'state={self.state.name}, assigned_node_ids={self.assigned_node_ids})'
        )

    def assign(self, nodes: Node | list[Node]) -> None:
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        node_list = nodes if isinstance(nodes, list) else [nodes]
        new_nodes = [node for node in node_list if node.identifier not in self.assigned_node_ids]
        self.assigned_node_ids.update(node.identifier for node in new_nodes)
        self.assigned_nodes.extend(new_nodes)
        self.modified = True

    def remove(self, nodes: Node | list[Node]) -> None:
        if self.kill_flag:
            raise RuntimeError('Job already flagged to be killed')
        node_list = nodes if isinstance(nodes, list) else [nodes]
        removed_node_ids = {node.identifier for node in node_list}
        self.assigned_node_ids -= removed_node_ids
        self.assigned_nodes = [node for node in self.assigned_nodes if node.identifier not in removed_node_ids]
        self.modified = True

    def kill(self) -> None:
        self.modified = True
        self.kill_flag = True

    def assign_num_gpus_per_node(self, assigned_num_gpus_per_node: int) -> None:
        self.assigned_num_gpus_per_node = assigned_num_gpus_per_node

    def update_runtime_argument(self, key: str, value: Any) -> None:
        self.modified = True
        self.modified_runtime_args = True
        self.runtime_arguments[key] = str(value)

    def to_dict(self) -> dict[str, Any]:
        json_dict: dict[str, Any] = dict(id=self.identifier, assigned_node_ids=[node.identifier for node in self.assigned_nodes],
                                         assigned_num_gpus_per_node=self.assigned_num_gpus_per_node, kill_flag=self.kill_flag,
                                         modified_runtime_args=self.modified_runtime_args)
        if self.modified_runtime_args:
            json_dict['runtime_arguments'] = self.runtime_arguments
            self.modified_runtime_args = False
        self.modified = False
        return json_dict
    
    def update(self, json_job: dict[str, Any], nodes: list[Node]) -> None:
        self._load(json_job, nodes)

    def _load(self, json_job: dict[str, Any], nodes: list[Node] = []) -> None:
        self.state: JobState = JobState(json_job['state'])
        self.start_time: float = json_job['start_time']
        self.end_time: float = json_job['end_time']
        self.wait_time: float = json_job['wait_time']
        self.makespan: float = json_job['makespan']
        self.turnaround_time: float = json_job['turnaround_time']
        self.assigned_node_ids: set[int] = set(json_job['assigned_nodes'])
        self.assigned_nodes: list[Node] = [nodes[node_id] for node_id in self.assigned_node_ids]
        self.assigned_num_gpus_per_node: int = json_job['assigned_num_gpus_per_node']
        self.runtime_arguments: dict[str, str] = json_job.get('runtime_arguments', {})
        self.total_phase_count: int = json_job['total_phase_count']
        self.completed_phases: int = json_job['completed_phases']
