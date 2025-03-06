import setuptools  # v61.0.0
import os  # standard library
import io  # standard library

# Get the absolute path of the current directory
here = os.path.abspath(os.path.dirname(__file__))

# Read the content of the README file
README = open(os.path.join(here, 'README.md'), encoding='utf-8').read()

def get_requirements():
    """
    Reads requirements from requirements.txt file
    
    Returns:
        list: List of package requirements
    """
    requirements_path = os.path.join(here, 'requirements.txt')
    with io.open(requirements_path, encoding='utf-8') as f:
        requirements = [
            line.strip() for line in f
            if line.strip() and not line.startswith('#')
        ]
    return requirements

setuptools.setup(
    name='electricity-market-price-forecasting',
    version='0.1.0',
    description='Electricity Market Price Forecasting System for day-ahead market price forecasts',
    long_description=README,
    long_description_content_type='text/markdown',
    author='Forecasting Team',
    author_email='forecasting@example.com',
    url='https://github.com/organization/electricity-market-price-forecasting',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=get_requirements(),
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Financial and Insurance Industry',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'License :: OSI Approved :: MIT License',
    ],
    python_requires='>=3.10',
    entry_points={
        'console_scripts': [
            'forecast-market-prices=backend.main:main',
        ],
    },
)