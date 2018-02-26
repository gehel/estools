from setuptools import setup

setup(name='estools',
      version='0.1',
      description='Tools to manage elasticsearch',
      url='http://github.com/gehel/estools',
      author='Guillaume Lederrey',
      author_email='guillaume.lederrey@wikimedia.org',
      license='Apache',
      packages=['estools'],
      install_requires=[
            'cumin',
            'python-dateutil',
            'pyyaml',
            'tqdm',
            'elasticsearch>=5.0.0,<6.0.0',
            'elasticsearch-curator>=5.0.0,<6.0.0',
            'git+ssh://git@github.com/wikimedia/operations-switchdc.git#egg=switchdc',
            'virtualfish',
            'freezegun',
            'mock'
      ],
      zip_safe=False)
