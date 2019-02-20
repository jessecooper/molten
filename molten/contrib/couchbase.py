# This file is a part of molten.
#
# Copyright (C) 2018 CLEARTYPE SRL <bogdan@cleartype.io>
#
# molten is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# molten is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from collections import namedtuple
from inspect import Parameter
from typing import Any, Callable, NewType, Optional

from molten import DependencyResolver, Settings

try:
    from couchbase.cluster import Cluster, PasswordAuthenticator
except ImportError:  # pragma: no cover
    raise ImportError("'couchbase' package missing. Run 'pip install couchbase'.")


class CouchbaseClusterComponent:
    """A component that sets up a Couchbase Cluster connection.  This component
    depends on the availability of a :class:`molten.Settings`
    component.

    Your settings dictionary must contain a ``cluster_host``, ``cluster_user``,
    ``cluster_password``. Additionally, you may
    provide a ``database_engine_params`` setting representing
    args that are passed to couchbase.Cluster.

    Examples:

      >>> from molten import App
      >>> from molten.contrib.couchbase import CouchbaseClusterComponent
      >>> from molten.contrib.toml_settings import TOMLSettingsComponent

      >>> app = App(
      ...   components=[
      ...     TOMLSettingsComponent(),
      ...     CouchbaseClusterComponent()
      ...   ],
      ... )
    """

    is_cacheable = True
    is_singleton = True

    def can_handle_parameter(self, parameter: Parameter) -> bool:
        return parameter.annotation is Cluster

    def resolve(self, settings: Settings) -> Cluster:
        cluster = Cluster(
            settings.strict_get("cluster_hosts")
        )

        authenticator = PasswordAuthenticator(
            settings.strict_get("cluster_user"),
            settings.strict_get("cluster_password")
        )
        cluster.authenticate(authenticator)

        return cluster

