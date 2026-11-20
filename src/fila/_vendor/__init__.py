# SPDX-FileCopyrightText: 2026 The fila Authors
# SPDX-License-Identifier: GPL-3.0-or-later

"""Third-party code that travels with fila, so that only the standard library
is needed on any node (section 4.2). Each package keeps its own license.

- ``tomli``: tomli 2.5.0 by Taneli Hukkinen, MIT license, copied unchanged from
  the ``tomli-2.5.0-py3-none-any.whl`` distribution on PyPI
  (https://github.com/hukkin/tomli). It reads TOML on Python 3.9 and 3.10,
  which lack ``tomllib``.
"""
