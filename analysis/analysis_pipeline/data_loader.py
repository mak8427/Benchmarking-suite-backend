"""
Helpers for reading HDF5 datasets into Polars DataFrames.
"""

from __future__ import annotations

from typing import Iterable, Iterator, List, Tuple

import h5py
import numpy as np
import polars as pl


def iter_datasets(
    node: h5py.Group, path_parts: List[str]
) -> Iterator[Tuple[List[str], h5py.Dataset]]:
    """Traverse *node* depth-first and yield dataset paths and objects.

    Args:
        node: Current HDF5 group to inspect.
        path_parts: List of path components accumulated so far.

    Yields:
        Tuples of updated path components and the discovered dataset.
    """
    for key, child in node.items():
        current_path = path_parts + [key]
        if isinstance(child, h5py.Dataset):
            yield current_path, child
        elif isinstance(child, h5py.Group):
            yield from iter_datasets(child, current_path)


def dataset_to_polars(dataset: h5py.Dataset) -> pl.DataFrame:
    """Convert an HDF5 dataset into a Polars DataFrame.

    Args:
        dataset: Dataset instance loaded from an HDF5 file.

    Returns:
        A Polars DataFrame representation of the dataset.
    """
    data = dataset[()]
    if np.isscalar(data):
        return pl.DataFrame({"value": [data]})

    if dataset.dtype.names:
        columns = {name: np.asarray(data[name]).tolist() for name in dataset.dtype.names}
        return pl.DataFrame(columns)

    array = np.asarray(data)
    if array.ndim == 1:

        return pl.DataFrame({"value": array.tolist()})

    rows, cols = array.shape[0], int(np.prod(array.shape[1:]))
    reshaped = array.reshape(rows, cols)
    column_names = [f"col_{idx}" for idx in range(cols)]
    columns = {name: reshaped[:, idx].tolist() for idx, name in enumerate(column_names)}
    return pl.DataFrame(columns)


def sanitize_parts(parts: Iterable[str]) -> str:
    """Join path components into a filesystem-friendly identifier.

    Args:
        parts: Iterable of path components to normalise.

    Returns:
        A flattened identifier separated by double underscores.
    """
    return "__".join(part.replace("/", "_") for part in parts)


def dataset_prefix(path_parts: List[str]) -> str:
    """Derive a dataset prefix from HDF5 path components.

    Args:
        path_parts: Ordered list of the dataset's path components.

    Returns:
        A stable identifier string derived from the path.
    """
    trimmed = path_parts[1:] if len(path_parts) > 1 else path_parts
    trimmed = [part for part in trimmed if part]
    if not trimmed:
        trimmed = path_parts
    return sanitize_parts(trimmed)
