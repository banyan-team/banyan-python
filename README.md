# Banyan Python

Banyan Python is an extension to the Julia programming language for that seamlessly scales existing libraries and code to massive data and compute. Banyan allows _partition types_ to be assigned to variables in existing code. Annotated code is automatically offloaded to run in a distributed fashion on managed clusters running in your AWS Virtual Private Cloud. Banyan optimizes code on-the-fly to take advantage of CPU caches and multicores in the clusters where the offloaded code runs.

<!-- Software libraries can be annotated with partition types and subsequent use of the annotated functions automatically runs at scale. Currently, we are developing two annotated libraries:

- [BanyanArrays.jl](https://www.banyancomputing.com/banyan-arrays-jl-docs)
- [BanyanDataFrames.jl](https://www.banyancomputing.com/banyan-data-frames-jl-docs)

Eventually, you will be able to use these libraries as drop-in replacements of the standard library Arrays and the DataFrames.jl library. By changing an import statement, you can run your code as is with Banyan scaling to arbitrary data or compute needs. -->

Visit [Banyan Computing](https://www.banyancomputing.com/resources/) for full documentation.

<!-- ## Getting Started

Banyan is the best way to unleash Julia on big data in the cloud! To get started:

1. Follow the [getting started steps](banyancomputing.com/getting-started) (15 minutes)
2. Create a cluster on the [dashboard](banyancomputing.com/dashboard)
3. Start a cluster session wherever you are running Julia with `start_session` (between 15s and 30min)
4. Use functions in [BanyanArrays.jl](https://www.banyancomputing.com/banyan-arrays-jl-docs) or [BanyanDataFrames.jl](https://www.banyancomputing.com/banyan-data-frames-jl-docs) for big data processing!
5. End the cluster session with `end_session`
6. Destroy the cluster on the [dashboard](banyancomputing.com/dashboard) -->

## Contributing

Please create branches named according the the author name and the feature name
like `{author-name}/{feature-name}`. For example: `caleb/add-tests-for-hdf5`.
Then, submit a pull request on GitHub to merge your branch into the branch with
the latest version number.

When pulling/pushing code, you may need to add the appropriate SSH key. Look
up GitHub documentation for how to generate an SSH key, then make sure to add
it. You may need to do this repeatedly if you have multiple SSH keys for
different GitHub accounts. For example, on Windows you may need to:

```
eval `ssh-agent`
ssh-add -D
ssh-add /c/Users/Claris/.ssh/id_rsa_clarisw
git remote set-url origin git@github.com:banyan-team/banyan-website.git
```

## Installation

Make sure you have Python installed and then run `poetry install` and `poetry update`
from the Banyan Python project you want to work on (e.g., `banyan-python` for banyan-python).

## Testing

To see an example of how to add tests, see `banyan-python/tests/test_config.py`.

To run tests, ensure that you have a Banyan account connected to an AWS account.
Then, `cd` into the directory with the Banyan Python project you want to run
tests for (e.g., `banyan-python` for banyan-python) and run:

- `poetry run pytest` to run all tests in `tests` directory
- `poetry run pytest tests/xx.py` to run all tests in `tests/xx.py`
- `poetry run pytest tests/xx.py::foo` to run `foo` function in `tests/xx.py`
- `poetry run pytest tests/xx.py::bar::foo` to run `foo` function in `bar` class in `tests/xx.py`

You must then specify the cluster name with the `BANYAN_CLUSTER_NAME`
environment variable. You must also specify the relevant `BANYAN_*`
and `AWS_*` environment variables to provide credentials. AWS
credentials are specified in the same way as they would be if using
the AWS CLI (either use environment variables or use the relevant
AWS configuration files) and the Banyan environment variables
are saved in `banyanconfig.toml` so you don't need to specify it
every time.

For example, if you have previously specified your Banyan API key, user ID, and AWS credentials, you could:

If your AWS credentials are saved under a profile named `banyan-testing`, you could use `AWS_DEFAULT_PROFILE=banyan-testing`.

<!-- ## Development

Make sure to use the `] dev ...` command or `Pkg.dev(...)` to ensure that when you
are using BanyanArrays.jl or BanyanDataFrames.jl you are using the local version. -->
