/bin/bash

# 安装 cryptography
pip install --no-index maturin-1.9.6-py3-none-manylinux_2_12_x86_64.manylinux2010_x86_64.musllinux_1_1_x86_64.whl
pip install --no-build-isolation ./flit_core-3.12.0.tar.gz
pip install --no-build-isolation ./pathspec-0.12.1.tar.gz
pip install --no-build-isolation ./pluggy-1.6.0.tar.gz
pip install --no-build-isolation ./hatchling-1.27.0.tar.gz
pip install --no-build-isolation ./puccinialin-0.1.5.tar.gz
pip install --no-build-isolation ./cryptography-46.0.3.tar.gz

# 安装 passlib
pip install --no-build-isolation ./passlib-1.7.4.tar.gz

# 安装 flask
pip install --no-build-isolation ./flask_cors-6.0.1.tar.gz
pip install --no-build-isolation ./pyjwt-2.10.1.tar.gz
pip install --no-build-isolation ./flask_jwt_extended-4.7.1.tar.gz
