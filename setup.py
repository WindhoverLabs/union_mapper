from setuptools import setup, find_packages

requires = [
    'PyYAML==5.3.1'
]

setup(
    name='union_mapper',
    version='1.0',
    packages=find_packages(),
    install_requires=requires,
    url='https://github.com/WindhoverLabs/union_mapper',
    license='License :: 3BSD-3-Clause',
    author='Lorenzo Gomez',
    author_email='lgomez@windhoverlabs.com',
    description=''
)
