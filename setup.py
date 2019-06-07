from setuptools import setup


def readme():
    with open('README.rst') as f:
        return f.read()


def version():
    with open('version.txt') as h:
        return h.read().strip()


setup(name='lcmap-blackmagic',
      version=version(),
      description='HTTP server that saves detected changes and predictions to Apache Cassandra for LCMAP',
      long_description=readme(),
      classifiers=[
        'Development Status :: 5 - Stable',
        'License :: OSI Approved :: Unlicense',
        'Programming Language :: Python :: 3.7',
      ],
      keywords='usgs lcmap eros pyccd ccdc',
      url='http://github.com/usgs-eros/lcmap-blackmagic',
      author='USGS EROS LCMAP',
      author_email='',
      license='Unlicense',
      packages=['blackmagic'],
      install_requires=[
          'arrow',
          'cython',
          'lcmap-merlin>=2.3.1',
          'lcmap-pyccd==2018.10.17',
          'xgboost',
          'flask',
          'gunicorn',
          'tenacity',
          'cassandra-driver'
          
      ],
      # List additional groups of dependencies here (e.g. development
      # dependencies). You can install these using the following syntax,
      # for example:
      # $ pip install -e .[test]
      extras_require={
          'test': ['pytest'],
          'dev': ['',],
      },
      #test_suite='nose.collector',
      #tests_require=['nose', 'nose-cover3'],
      entry_points={
          'console_scripts': ['blackmagic=blackmagic.app:main'],
      },
      include_package_data=True,
      zip_safe=False)
