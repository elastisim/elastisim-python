# This file is part of the ElastiSim software.
#
# Copyright (c) 2023, Technical University of Darmstadt, Germany
#
# This software may be modified and distributed under the terms of the 3-Clause
# BSD License. See the LICENSE file in the base directory for details.

from __future__ import annotations
from enum import Enum
from typing import Any


class GpuState(Enum):
    FREE = 0
    ALLOCATED = 1


class Gpu:
    identifier: int = None
    state: GpuState = None

    def __init__(self, gpu: dict[str, int]):
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
    identifier: int = None
    type: NodeType = None
    state: NodeState = None
    assigned_jobs: list[Job] = None
    assigned_job_ids: set[int] = None
    gpus: list[Gpu] = None

    def __init__(self, node: dict[str, Any]):
        self.identifier = node['id']
        self.type = NodeType(node['type'])
        self.state = NodeState(node['state'])
        self.assigned_jobs = None
        self.assigned_job_ids = set(node['assigned_jobs'])
        self.gpus = [Gpu(gpu) for gpu in node['gpus']]
