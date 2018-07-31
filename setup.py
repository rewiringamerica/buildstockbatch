import setuptools

setuptools.setup(
    name='buildstock-batch',
    version='0.0.1',
    author='Noel Merket (NREL)',
    author_email='noel.merket@nrel.gov',
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    package_data={
        'buildstockbatch': ['*.sh']
    },
    install_requires=[
        'pyyaml',
        'requests',
        'pandas',
        'joblib',
        'feather-format'
    ],
    entry_points={
        'console_scripts': [
            'buildstock_docker=buildstockbatch.localdocker:main',
            'buildstock_peregrine=buildstockbatch.peregrine:main'
        ]
    }
)
