from setuptools import setup


setup(
    name="trimble-geospatial-demo-utils",
    version="0.1.0",
    description="Trimble geospatial demo utility helpers.",
    packages=["trimble_geospatial_demo_utils"],
    package_dir={"trimble_geospatial_demo_utils": "."},
    python_requires=">=3.8",
    install_requires=["requests>=2.0.0"],
)
