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
    from .job import Job


class GpuState(Enum):
    FREE = 0
    ALLOCATED = 1


class Gpu:
    def __init__(self, json_gpu: dict[str, int]) -> None:
        self.identifier: int = json_gpu['id']
        self.state: GpuState = GpuState(json_gpu['state'])


class NodeType(Enum):
    COMPUTE_NODE = 0
    COMPUTE_NODE_WITH_BB = 1
    COMPUTE_NODE_WIDE_STRIPED_BB = 2


class NodeState(Enum):
    FREE = 0
    ALLOCATED = 1
    RESERVED = 2


class Node:
    def __init__(self, json_node: dict[str, Any]) -> None:
        self.identifier: int = json_node['id']
        self.type: NodeType = NodeType(json_node['type'])
        self._load(json_node)
    
    def __eq__(self, other) -> bool:
        if isinstance(other, Node):
            return self.identifier == other.identifier
        return False

    def __hash__(self) -> int:
        return hash(self.identifier)
    
    def __lt__(self, other: Node) -> bool:
        return self.identifier < other.identifier

    def __repr__(self) -> str:
        return (
            f'Node(id={self.identifier}, type={self.type.name}, '
            f'state={self.state.name}, assigned_job_ids={self.assigned_job_ids})'
        )

    def update(self, json_node: dict[str, Any], jobs: list[Job]) -> None:
        self._load(json_node, jobs)

    def _load(self, json_node: dict[str, Any], jobs: list[Job] = []) -> None:
        self.state: NodeState = NodeState(json_node['state'])
        self.assigned_job_ids: set[int] = set(json_node['assigned_jobs'])
        self.assigned_jobs: list[Job] = [jobs[job_id] for job_id in self.assigned_job_ids]
        self.gpus: list[Gpu] = [Gpu(json_gpu) for json_gpu in json_node['gpus']]
