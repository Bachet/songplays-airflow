import setuptools

setuptools.setup(
    name="airflow_udacity_plugin",
    packages=setuptools.find_packages(),
    entry_points={"airflow.plugins": ["udacity_plugin = plugins.__init__:UdacityPlugin"]},
    version="0.0.1",
)
