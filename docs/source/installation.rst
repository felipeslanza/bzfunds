Installation
============

To use this project, simply clone the `repository
<https://github.com/felipeslanza/bzfunds>` and install the dependencies.

.. code-block:: shell

    git clone git@github.com:felipeslanza/bzfunds.git

Requirements
------------

This projects requires:

    * `python >= 3.8`
    * `MongoDB >= 4.0`


Setup
------

Create a `virtualenv` and install the dependencies.

.. code-block:: shell

    virtualenv -p pytnon3.8 venv
    source ./venv/bin/active
    pip install -r requirements.txt

Further instructions regarding your `MongoDB <https://docs.mongodb.com/>`_ setup
can be found at the :doc:`User Guide <user_guide>`.

If you are running a MongoDB instance on the cloud, you will likely require `dns-python`
(used by `pymongo`), which is included in the project's requirements by default. If you're
unable to install it, you can try re-installing `pymongo` directly through ``pip install
pymongo[srv]``.

Most project-level configuration, including any MongoDB-related settings, can be modified
at will at :py:mod:`bzfunds.settings.py <bzfunds.settings>`.

