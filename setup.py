from setuptools import setup

setup(
    name="k-snap",
    version="1.0.0",
    description="A space-efficient K8s object history recorder and browser using Delta snapshots.",
    author="Manvendra",
    py_modules=["ksnap"],
    install_requires=[
        "pyyaml>=6.0",
    ],
    entry_points={
        "console_scripts": [
            "ksnap=ksnap:main",
        ],
    },
    python_requires=">=3.7",
)
