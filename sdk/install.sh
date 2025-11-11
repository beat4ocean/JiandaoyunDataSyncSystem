/bin/bash

# 安装 cryptography
pip install --no-index --no-build-isolation maturin-1.9.6-py3-none-manylinux_2_12_x86_64.manylinux2010_x86_64.musllinux_1_1_x86_64.whl
pip install --no-index --no-build-isolation ./flit_core-3.12.0.tar.gz
pip install --no-index --no-build-isolation ./pathspec-0.12.1.tar.gz
pip install --no-index --no-build-isolation ./pluggy-1.6.0.tar.gz
pip install --no-index --no-build-isolation ./trove_classifiers-2025.9.11.17.tar.gz
pip install --no-index --no-build-isolation ./hatchling-1.27.0.tar.gz
pip install --no-index --no-build-isolation ./puccinialin-0.1.5.tar.gz
pip install --no-index --no-build-isolation ./hatchling-1.27.0.tar.gz
pip install --no-index --no-build-isolation ./cryptography-46.0.3.tar.gz
pip install --no-deps ./cryptography-46.0.3.tar.gz

# 安装 passlib
pip install --no-index --no-build-isolation ./passlib-1.7.4.tar.gz

# 安装 flask
pip install --no-index --no-build-isolation ./flask_cors-6.0.1.tar.gz
pip install --no-index --no-build-isolation ./pyjwt-2.10.1.tar.gz
pip install --no-index --no-build-isolation ./flask_jwt_extended-4.7.1.tar.gz

# 安装 postgres 驱动
#pip install --no-index --no-build-isolation ./psycopg2_binary-2.9.11-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.whl
pip install --no-deps oracledb-3.4.0-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl

# 安装 oracle 驱动
pip install --no-index --no-build-isolation ./oracledb-3.4.0-cp312-cp312-manylinux2014_x86_64.manylinux_2_17_x86_64.manylinux_2_28_x86_64.whl
