from setuptools import setup
import os

with open('README.rst') as f:
    readme = f.read()

with open(os.path.abspath("hams_admin/VERSION.txt"), "r") as fp:
    version = fp.read().strip()

setup(
    name='hams_admin',
    version=version,
    description='Admin commands',
    long_description=readme,
    maintainer='Shixiong Zhao',
    maintainer_email='sxzhao@cs.hku.hk',
    url='test',
    license='Apache-2.0',
    packages=[
        "hams_admin", "hams_admin.docker", 
        "hams_admin.deployers"
    ],
    package_data={'hams_admin': ['*.txt', '*/*.yaml']},
    keywords=['hams', 'prediction', 'model', 'management'],
    install_requires=[
        'requests', 'numpy', 'subprocess32; python_version<"3"', 'pyyaml',
        'docker>=3.0', 'kubernetes>=6.0.0', 'prometheus_client',
        'cloudpickle==0.5.3', 'enum34; python_version<"3.4"', 'redis', 'psutil',
        'jsonschema', 'jinja2'
    ],
    extras_require={
        'PySpark': ['pyspark'],
        'TensorFlow': ['tensorflow'],
    })
