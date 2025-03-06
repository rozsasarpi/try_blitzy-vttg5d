import setuptools  # setuptools version: latest
import os  # os version: standard library
import io  # io version: standard library

# Get the absolute path of the directory containing this file
here = os.path.abspath(os.path.dirname(__file__))

# Read the content of README.md file for long description
try:
    with open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        README = f.read()
except FileNotFoundError:
    README = "Dash-based visualization interface for the Electricity Market Price Forecasting System"

def get_requirements():
    """
    Reads requirements from requirements.txt file
    
    Returns:
        list: List of package requirements
    """
    requirements = []
    try:
        with io.open(os.path.join(here, 'requirements.txt'), encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                # Skip empty lines and comments
                if line and not line.startswith('#'):
                    requirements.append(line)
    except FileNotFoundError:
        # Fallback minimal requirements if file not found
        requirements = [
            "dash>=2.9.0",
            "plotly>=5.14.0",
            "pandas>=2.0.0",
            "pandera>=0.16.0",
            "numpy>=1.24.0",
        ]
    
    return requirements

setuptools.setup(
    name="electricity-market-price-forecasting-dashboard",
    version="0.1.0",
    description="Dash-based visualization interface for the Electricity Market Price Forecasting System",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Forecasting Team",
    author_email="forecasting@example.com",
    url="https://github.com/organization/electricity-market-price-forecasting",
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=get_requirements(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "License :: OSI Approved :: MIT License",
        "Framework :: Dash",
    ],
    python_requires=">=3.10",
    entry_points={
        "console_scripts": [
            "forecast-dashboard=web.app:run_server",
        ],
    },
)