#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Exports a conda dependency graph into Neo4j Aura
"""

import itertools
import json
import os
import subprocess
import tqdm
from neo4j import GraphDatabase, Transaction
from typing import Any, Dict, List, Set, Tuple


class Neo4jAuraDb:

    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def run(self, query: str) -> None:
        with self.driver.session() as session:
            return session.read_transaction(self._run_query, query)

    @staticmethod
    def _run_query(tx: Transaction, query: str) -> Any:
        return tx.run(query).data()


def _parse_linked_data(data: Dict[Any, Any]) -> Any:
    g: Dict[str, Tuple[str, List[Tuple[str, str]]]] = {}
    for k in data.keys():
        sn = data[k]['name']
        g[sn] = (data[k]['version'], [])
        for dep in data[k]['depends']:
            dep_lst = dep.split(' ')
            g[sn][1].append((dep_lst[0], dep_lst[1:]))

    return g


def _get_current_deps() -> Dict[Any, Any]:
    import conda.exports
    conda_cmd = os.environ.get('CONDA_EXE', 'conda')
    info = json.loads(subprocess.check_output([conda_cmd, 'info', '-e', '--json']))
    if 'active_prefix' not in info:
        raise ValueError('Cannot get the current active conda environment ID')

    deps = conda.exports.linked_data(prefix=info['active_prefix'])
    return _parse_linked_data(deps)


def _extract_root_packages(deps: Dict[str, Any]) -> Set[str]:
    pkgs = itertools.chain.from_iterable(map(lambda p: map(lambda dp: dp[0], p[1]), deps.values()))
    return set(deps.keys()) - set(pkgs)


def _build_cypher_queries_from(deps: Dict[str, Any]) -> List[str]:
    root_packages = _extract_root_packages(deps)
    create_stmts = []
    for pkg, (version, _) in deps.items():
        node_label = "RootPackage" if pkg in root_packages else "Package"
        create_stmts.append(f"CREATE (n:{node_label} {{name:'{pkg}', version:'{version}'}});")

    for pkg, (_, lst) in deps.items():
        for rpkg, required in lst:
            create_stmts.append(f"MATCH (src), (dst:Package) WHERE src.name = '{pkg}' AND dst.name = '{rpkg}' "
                                f"CREATE (src)-[:provided {{required:'{''.join(required)}'}}]->(dst);")

    return create_stmts


def main() -> None:
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--uri', type=str, required=True)
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--password', type=str, required=True)
    parser.add_argument('--dryrun', action='store_true')
    args = parser.parse_args()

    create_stmts = _build_cypher_queries_from(_get_current_deps())

    if not args.dryrun:
        neo4jdb = Neo4jAuraDb(args.uri, args.user, args.password)
        for create_stmt in tqdm.tqdm(create_stmts, desc='CYPHER CREATE Stmts'):
            neo4jdb.run(create_stmt)
    else:
        for create_stmt in create_stmts:
            print(create_stmt)


if __name__ == "__main__":
    main()
