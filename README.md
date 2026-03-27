# Book To Sound (Go)

上传电子书/文档，自动按章节切分并批量导出音频，最后打包成 ZIP 下载。
## 图书下载地址
[https://zh.z-library.sk/](https://zh.z-library.sk/)

## 功能

- Web 上传 `AZW3/EPUB/MOBI/PDF/TXT`
- 自动章节识别（`第X章/第X节/Chapter X`）
- 按章节导出音频
- 实时进度（章节进度、已耗时、预计剩余时间）
- 支持导出格式：
  - `m4a/mp4`（推荐，跨平台兼容好；`mp4` 参数会按音频容器输出为 `m4a` 文件）
  - `mp3`（需要 `ffmpeg`）

## 依赖

### 必需

- Go `1.23+`

### macOS

- 系统自带 `say`（已内置）
- 若导出 `mp3`：安装 `ffmpeg`
- 若上传 `AZW3/EPUB/MOBI`：安装 Calibre（`ebook-convert`）

### Linux

- `espeak-ng` 或 `espeak`
- `ffmpeg`（`mp3/m4a` 都需要）
- 若上传 `AZW3/EPUB/MOBI`：安装 Calibre（`ebook-convert`）

## 安装命令

### macOS (Homebrew)

```bash
brew install ffmpeg
brew install --cask calibre
```

安装后检查：

```bash
which ffmpeg
which ebook-convert
```

### Ubuntu / Debian

```bash
sudo apt update
sudo apt install -y ffmpeg espeak-ng calibre
```

安装后检查：

```bash
which ffmpeg
which ebook-convert
which espeak-ng
```

## 启动

```bash
go mod tidy
go run .
```

浏览器打开：`http://localhost:8080`

## 接口

- `POST /api/jobs`：上传并创建转换任务
- `GET /api/jobs/{jobID}`：查询任务进度和预计剩余时间
- `GET /api/jobs/{jobID}/download`：任务完成后下载 ZIP

## 说明

- 如果 PDF 是扫描件（图片型 PDF），本项目无法直接提取文字，需要先 OCR。
- 如果是 `AZW3/EPUB/MOBI`，依赖 `ebook-convert` 做文本抽取。
- 若章节标题不规范，程序会自动按固定长度切分为 `Part_01/Part_02...`。
- 音频导出较耗时，取决于文档长度和机器性能。
