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
Exports a Python dependency graph into Neo4j
"""

import itertools
import tqdm
from neo4j import GraphDatabase, Transaction
from typing import Any, Dict, List, Set, Tuple


class Neo4jDb:

    def __init__(self, uri: str, user: str, password: str) -> None:
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self) -> None:
        self.driver.close()

    def run(self, queries: List[str]) -> None:
        with self.driver.session() as session:
            for q in tqdm.tqdm(queries, desc='Neo4j Queries'):
                session.read_transaction(self._run_query, q)

    @staticmethod
    def _run_query(tx: Transaction, query: str) -> Any:
        return tx.run(query).data()


def _get_pip_deps() -> Dict[str, Any]:
    import pipdeptree
    g: Dict[str, Tuple[str, List[Tuple[str, List[str]]]]] = {}
    deps = pipdeptree.get_installed_distributions(local_only=False, user_only=False)
    dep_tree = pipdeptree.PackageDAG.from_pkgs(deps)
    for k, v in dep_tree.items():
        sn = k.key
        g[sn] = (k.version, [])
        for dep in v:
            g[sn][1].append((dep.key, list(map(lambda p: ''.join(p), dep.specs))))

    return g


def _parse_linked_data(data: Dict[Any, Any]) -> Any:
    g: Dict[str, Tuple[str, List[Tuple[str, List[str]]]]] = {}
    for k, v in data.items():
        sn = v['name']
        g[sn] = (v['version'], [])
        for dep in v['depends']:
            dep_lst = dep.split(' ')
            specs = list(itertools.chain.from_iterable(map(lambda r: r.split(","), dep_lst[1:])))
            g[sn][1].append((dep_lst[0], specs))

    return g


def _get_conda_deps(conda_env_prefix: str) -> Dict[str, Any]:
    try:
        import conda.exports
    except ImportError:
        raise RuntimeError('You need to install conda first')

    deps = conda.exports.linked_data(prefix=conda_env_prefix)
    if not deps:
        raise ValueError(f"No dependency found in conda env prefix '{conda_env_prefix}'")

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
        for rpkg, requires in lst:
            req_versions = ','.join(map(lambda v: f"'{v}'", requires))
            create_stmts.append(f"MATCH (src), (dst:Package) WHERE src.name = '{pkg}' AND dst.name = '{rpkg}' "
                                f"CREATE (src)-[:provided {{requires:[{req_versions}]}}]->(dst);")

    return create_stmts


def main() -> None:
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('--uri', type=str, required=True)
    parser.add_argument('--user', type=str, required=True)
    parser.add_argument('--password', type=str, required=True)
    parser.add_argument('--conda-env-prefix', type=str)
    parser.add_argument('--dryrun', action='store_true')
    args = parser.parse_args()

    deps = _get_conda_deps(str(args.conda_env_prefix)) if args.conda_env_prefix \
        else _get_pip_deps()

    create_stmts = _build_cypher_queries_from(deps)

    if not args.dryrun:
        neo4jdb = Neo4jDb(args.uri, args.user, args.password)
        neo4jdb.run(create_stmts)
    else:
        for create_stmt in create_stmts:
            print(create_stmt)


if __name__ == "__main__":
    main()
