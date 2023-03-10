"""Module for housing post-processing operations."""

from abc import abstractproperty, abstractmethod, ABC
from datetime import datetime

from monty.json import MSONable
import numpy as np
import pandas as pd

# https://python-semver.readthedocs.io/en/stable/api.html#semver.match
# import semver

from scipy.interpolate import InterpolatedUnivariateSpline
from larch import Group as xafsgroup
from larch.xafs import pre_edge


class SpecsCompatibilityError(Exception):
    def __init__(self, node_spec_names, operator_spec_names):
        message = (
            f"Node specs are {node_spec_names}, but the operator "
            f"{operator_spec_names}"
        )
        super().__init__(message)


class SpecsVersionCompatibilityError(Exception):
    def __init__(self, node_spec_names, operator_spec_names):
        message = (
            f"Node specs are {node_spec_names}, but the operator "
            f"{operator_spec_names}"
        )
        super().__init__(message)


class Operator(MSONable):
    """Base operator class. Tracks everything required through a combination
    of the MSONable base class and by using an additional datetime key to track
    when the operator was logged into the metadata of a new node.

    .. important::

        The __call__ method must be derived for every operator. In particular,
        this operator should take as arguments at least one data point (node).
    """

    def _process_data(self, df, metadata):
        return df

    def _process_metadata(self, df, metadata):
        return metadata

    def __call__(self, df, metadata):
        df = self._process_data(df, metadata)
        metadata = self._process_metadata(df, metadata)
        return df, metadata


class UnaryOperatorOnNodeMixin(MSONable, ABC):
    """This mixin class defines methods on ``Node`` objects. It will do a few
    things:

    - Requires a ``requirements`` property, which returns the list of required
      specs that the node must have, and their versions.
    - Defines compatibility between versions
    """

    @property
    @abstractproperty
    def requirements(self):
        """Returns a minimum list of requirements of the form

        .. code::

            [
                {"name": SPEC_NAME, "version": None},
                {"name": SPEC_NAME_2, "version": "==0.0.1"},
                {"name": SPEC_NAME_3, "version": ">1.0.5"}
                ...
            ]

        which will be checked against the node at runtime to ensure that the
        operator is compatible with that verison of the spec.

        Returns
        -------
        dict
        """

        ...

    @abstractmethod
    def _process_data(self, node):
        """Processes the data on the node.

        Parameters
        ----------
        node : tiled.client.node.Node

        Returns
        -------
        pd.DataFrame
        """

        ...

    @abstractmethod
    def _process_metadata(self, node):
        """Processes the metadata on the node, but excludes any processing
        specific to tracking provenance.

        Parameters
        ----------
        node : tiled.client.node.Node

        Returns
        -------
        dict
        """

        ...

    def _assert_compatibility(self, node):
        # Get the current node's specs
        node_specs = node.item["attributes"]["specs"]

        # Compare those specs to the requirements of the operator
        operator_specs = self.requirements

        operator_specs_names = set([xx["name"] for xx in node_specs])
        node_specs_names = set([xx["name"] for xx in operator_specs])

        # If there is a mismatch between the required specs and the node specs
        # immediately exit
        if not node_specs_names.issuperset(operator_specs_names):
            raise SpecsCompatibilityError(
                node_specs_names, operator_specs_names
            )

        # Compare the versions
        # TODO

    def _preprocess(self, node):
        """Processes the parent node's metadata.

        Parameters
        ----------
        node : tiled.client.node.Node

        Returns
        -------
        dict
            Operator-specific metadata for the new object.
        """

        self._assert_compatibility(node)

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        return {
            "operator": self.as_dict(),
            "dt": now,
            "parent": node.item["id"],
        }

    def __call__(self, node):
        provenance = self._preprocess(node)
        new_metadata = self._process_metadata(node)
        new_data = self._process_data(node)
        postprocessed = {"operator_information": provenance}
        return new_data, {**new_metadata, **postprocessed}


class MetadataOnlyUnaryOperatorOnNodeMixin(UnaryOperatorOnNodeMixin):
    """Defines methods on operators that act in-place"""

    def _process_data(self, node):
        """No data is changed in the metadata-only operators.

        Parameters
        ----------
        node : tiled.client.node.Node

        Returns
        -------
        None
        """

        return None

    def __call__(self, node):
        new_data, new_metadata = super().__call__(node)

        # For in-place operations, we don't require the parent info
        new_metadata["operator_information"].pop("parent")

        return new_data, new_metadata


class StandardizeGrid(Operator):
    """Interpolates specified columns onto a common grid.

    Parameters
    ----------
    x0 : float
        The lower bound of the grid to interpolate onto.
    xf : float
        The upper bound of the grid to interpolate onto.
    nx : int
        The number of interpolation points.
    interpolated_univariate_spline_kwargs : dict, optional
        Keyword arguments to be passed to the
        :class:`InterpolatedUnivariateSpline`. See
        [here](https://docs.scipy.org/doc/scipy/reference/generated/
        scipy.interpolate.InterpolatedUnivariateSpline.html) for the
        documentation on this class.
    x_column : str, optional
        References a single column in the DataFrameClient (this is the
        "x-axis").
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes").
    """

    def __init__(
        self,
        *,
        x0,
        xf,
        nx,
        interpolated_univariate_spline_kwargs=dict(),
        x_column="energy",
        y_columns=["mu"],
    ):
        self.x0 = x0
        self.xf = xf
        self.nx = nx
        self.interpolated_univariate_spline_kwargs = (
            interpolated_univariate_spline_kwargs
        )
        self.x_column = x_column
        self.y_columns = y_columns

    def _process_data(self, df, _):
        """Takes in a dictionary of the data amd metadata. The data is a
        :class:`pd.DataFrame`, and the metadata is itself a dictionary.
        Returns the same dictionary with processed data and metadata.
        """

        new_grid = np.linspace(self.x0, self.xf, self.nx)
        new_data = {self.x_column: new_grid}
        for column in self.y_columns:
            ius = InterpolatedUnivariateSpline(
                df[self.x_column],
                df[column],
                **self.interpolated_univariate_spline_kwargs,
            )
            new_data[column] = ius(new_grid)

        return pd.DataFrame(new_data)


class NormalizeLarch(Operator):
    """Return XAS spectrum normalized using larch.
    Post-edge is normalized such that spectral features oscillate around 1.

    Calls larch `pre_edge` function on data to perfrom normalization.
    This function performs several steps:
       1. determine E0 (if not supplied) from max of deriv(mu)
       2. fit a line to the region below the edge
       3. fit a quadratic curve to the region above the edge
       4. extrapolate the two curves to E0 and take their difference
          to determine the edge jump

    Normalized spectrum (`norm_mu`) is calculated via the following:
    `norm_mu = (mu - pre_edge_line) / edge_jump`

    To flatten the spectrum the fitted post-edge quadratic curve is subtracted
    from the post-edge.


    Parameters
    ----------
    x_column : str, optional
        References a single column in the DataFrameClient (this is the
        "x-axis"). Default is "energy".
    y_columns : list, optional
        References a list of columns in the DataFrameClient (these are the
        "y-axes"). Default is ["mu"].
    larch_pre_edge_kwargs : dict, optional
        Dictionary of keyword arguments to be passed into larch pre_edge
        function. Can be used to specify normalization parameters that are
        otherwise calculated in larch (e.g., e0, edge jump size, pre-edge
        range, etc.). See https://xraypy.github.io/xraylarch/xafs_preedge.html
        for pre_edge documentation.
    """

    def __init__(
        self,
        *,
        x_column="energy",
        y_columns=["mu"],
        larch_pre_edge_kwargs=dict(),
    ):
        self.x_column = x_column
        self.y_columns = y_columns
        self.larch_pre_edge_kwargs = larch_pre_edge_kwargs

    def _process_data(self, df, _):
        new_data = {self.x_column: df[self.x_column]}
        for column in self.y_columns:
            larch_group = xafsgroup()
            larch_group.energy = np.array(df[self.x_column])
            larch_group.mu = np.array(df[column])
            pre_edge(
                larch_group,
                group=larch_group,
                **self.larch_pre_edge_kwargs,
            )
            norm_mu = larch_group.flat
            new_data.update({column: norm_mu})

        return pd.DataFrame(new_data)


class XASDataQuality(MetadataOnlyUnaryOperatorOnNodeMixin):
    """Label the spectrum as "good", "bad" or "ugly"."""

    @property
    def requirements(self):
        return [{"name": "ExperimentalXAS", "version": None}]

    def __init__(self, negative_threshold=0.2, tail_positive_threshold=0.02):
        self._negative_threshold = negative_threshold
        self._tail_positive_threshold = tail_positive_threshold

    def _process_metadata(self, node):
        df = node.read()
        metadata = dict(node.metadata)
        c1 = (df["mu"].to_numpy() < 0.0).mean() > self._negative_threshold
        mu = (df["mu"].to_numpy()[-len(df.index) // 4 :] > 1.5).mean()
        c2 = mu > self._tail_positive_threshold
        if c1 or c2:
            metadata["quality"] = "ugly"
        else:
            metadata["quality"] = "good"
        return metadata


# # TODO
# class PreNormalize(Operator):
#     ...


# def postprocess(client, operators=[], pbar=True):
#     operators_string = "->".join([xx.__class__.__name__ for xx in operators])
#     operators_details = [operator.as_dict() for operator in operators]
#     for parent_uid, node in tqdm(client.items(), disable=not pbar):
#         df = node.read()
#         new_metadata = dict(node.metadata).copy()
#         for operator in operators:
#             df, new_metadata = operator(df, new_metadata)

#         # Append some specific postprocessing information to the results
#         dt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
#         new_metadata["operator_details"] = operators_details
#         new_metadata["modified"] = dt
#         new_metadata["dataset"] = operators_string
#         client.write_dataframe(df, metadata=new_metadata)
