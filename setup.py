from setuptools import setup, find_packages
import re


def get_version():
    with open("dynamodb_not_orm/__init__.py", "r", encoding="utf-8") as fh:
        content = fh.read()
        match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
        if match:
            return match.group(1)
        raise RuntimeError(
            "Could not find __version__ in dynamodb_not_orm/__init__.py"
        )


with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [
        line.strip()
        for line in fh
        if line.strip() and not line.startswith("#")
    ]


setup(
    name="dynamodb-not-orm",
    version=get_version(),
    author="Timophey Akulich",
    author_email="tima.akulich@gmail.com",
    description="A lightweight DynamoDB ORM alternative",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/timaakulich/dynamodb-not-orm",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
    ],
    python_requires=">=3.8",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "not-orm=not_orm_cli:app",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.txt", "*.md"],
    },
)
