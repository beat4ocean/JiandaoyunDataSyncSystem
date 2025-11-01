#!/bin/bash
# 该脚本将文件下载到 ./frontend/vendor/ 目录
# 增加了 -L 参数以跟随重定向

TARGET_DIR="frontend/vendor"

# 1. 创建目录 (如果不存在)
echo "正在创建目录: $TARGET_DIR"
mkdir -p "$TARGET_DIR"

# 2. 下载文件 (使用 -L 跟随重定向)
echo "正在下载 [1/7] index.css..."
curl -L -o "$TARGET_DIR/index.css" "https://unpkg.com/element-plus/dist/index.css"

echo "正在下载 [2/7] vue.global.js..."
curl -L -o "$TARGET_DIR/vue.global.js" "https://unpkg.com/vue@3/dist/vue.global.js"

echo "正在下载 [3/7] icons-vue.iife.js..."
curl -L -o "$TARGET_DIR/icons-vue.iife.js" "https://unpkg.com/@element-plus/icons-vue/dist/index.iife.js"

echo "正在下载 [4/7] element-plus.full.js..."
curl -L -o "$TARGET_DIR/element-plus.full.js" "https://unpkg.com/element-plus/dist/index.full.js"

echo "正在下载 [5/7] zh-cn.min.js..."
curl -L -o "$TARGET_DIR/zh-cn.min.js" "https://unpkg.com/element-plus/dist/locale/zh-cn.min.js"

echo "正在下载 [6/7] vue-router.global.js..."
curl -L -o "$TARGET_DIR/vue-router.global.js" "https://unpkg.com/vue-router@4/dist/vue-router.global.js"

echo "正在下载 [7/7] axios.min.js..."
curl -L -o "$TARGET_DIR/axios.min.js" "https://unpkg.com/axios/dist/axios.min.js"

echo "所有文件下载完成。"
echo "请检查 $TARGET_DIR 目录中的文件大小，它们不应是 1KB 之类的小文件。"