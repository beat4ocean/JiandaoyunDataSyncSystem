FROM python:3.13-slim

# 设置工作目录
WORKDIR /app

# 设置时区为上海
ENV TZ=Asia/Shanghai

# 删除默认源文件（包括 sources.list 和 sources.list.d/ 下的内容）
RUN rm -f /etc/apt/sources.list && \
    rm -rf /etc/apt/sources.list.d/*

# 写入阿里云源（适配 Debian trixie）
RUN echo "deb https://mirrors.aliyun.com/debian/ trixie main" > /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian/ trixie-updates main" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.aliyun.com/debian-security trixie-security main" >> /etc/apt/sources.list

# 安装 tzdata 和 bash，并设置时区
RUN apt-get update && apt-get install -y --no-install-recommends bash tzdata \
    && ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime \
    && dpkg-reconfigure -f noninteractive tzdata \
    && rm -rf /var/lib/apt/lists/*

# 将依赖文件复制到工作目录
COPY requirements.txt .

# 安装依赖
RUN pip install --no-cache-dir -r requirements.txt -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com

# 将当前目录内容复制到容器的 /app 目录下
COPY . .

# 定义容器启动时运行的命令
CMD ["python", "-u", "run.py"]
