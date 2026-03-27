package main

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ledongthuc/pdf"
)

const (
	maxUploadSize      = 200 << 20 // 200MB
	maxPDFTextRuneSize = 400000
	jobTTL             = 2 * time.Hour
)

var (
	errUnsupportedAudioFormat = errors.New("unsupported audio format")
	errNeedFFmpeg             = errors.New("ffmpeg is required for this output format")
	errNeedEbookConvert       = errors.New("ebook-convert (calibre) is required for this input format")
	reHanSpaceHan             = regexp.MustCompile(`([\p{Han}])\s+([\p{Han}])`)
	reHanSpaceClosePunct      = regexp.MustCompile(`([\p{Han}])\s+([，。！？；：、）》】”’])`)
	reOpenPunctSpaceHan       = regexp.MustCompile(`([（《【“‘])\s+([\p{Han}])`)
	chapterRegexes            = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*chapter\s+\d+[\s:：-]?.*$`),
		regexp.MustCompile(`^\s*第[0-9一二三四五六七八九十百千万零〇两]+章[\s:：-]?.*$`),
		regexp.MustCompile(`^\s*第[0-9一二三四五六七八九十百千万零〇两]+节[\s:：-]?.*$`),
	}
	supportedInputExt = map[string]bool{
		".pdf":  true,
		".txt":  true,
		".epub": true,
		".mobi": true,
		".azw3": true,
		".azw":  true,
	}
)

type chapter struct {
	Title string
	Text  string
}

type jobStatus string

const (
	jobQueued  jobStatus = "queued"
	jobRunning jobStatus = "running"
	jobDone    jobStatus = "done"
	jobFailed  jobStatus = "failed"
)

type convertJob struct {
	ID                 string    `json:"job_id"`
	Status             jobStatus `json:"status"`
	Stage              string    `json:"stage"`
	Error              string    `json:"error,omitempty"`
	SourceName         string    `json:"source_name"`
	Format             string    `json:"format"`
	Voice              string    `json:"voice"`
	Rate               int       `json:"rate"`
	TotalChapters      int       `json:"total_chapters"`
	DoneChapters       int       `json:"done_chapters"`
	CurrentChapter     string    `json:"current_chapter"`
	Progress           float64   `json:"progress"`
	EstimatedTotalSec  int64     `json:"estimated_total_sec"`
	EstimatedRemainSec int64     `json:"estimated_remain_sec"`
	ElapsedSec         int64     `json:"elapsed_sec"`
	DownloadURL        string    `json:"download_url,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`

	WorkDir         string
	SourcePath      string
	SourceExt       string
	ZipPath         string
	totalRunes      int
	processingStart time.Time
	cleanupDeadline time.Time
}

type jobStore struct {
	mu   sync.RWMutex
	jobs map[string]*convertJob
}

func newJobStore() *jobStore {
	return &jobStore{jobs: make(map[string]*convertJob)}
}

func (s *jobStore) set(job *convertJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *jobStore) get(id string) (*convertJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *jobStore) update(id string, fn func(j *convertJob)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	job, ok := s.jobs[id]
	if !ok {
		return false
	}
	fn(job)
	job.UpdatedAt = time.Now()
	return true
}

func (s *jobStore) snapshot(id string) (*convertJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, false
	}
	cp := *j
	return &cp, true
}

func (s *jobStore) cleanupExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, job := range s.jobs {
		if (job.Status == jobDone || job.Status == jobFailed) && now.After(job.cleanupDeadline) {
			if job.WorkDir != "" {
				_ = os.RemoveAll(job.WorkDir)
			}
			delete(s.jobs, id)
		}
	}
}

var jobs = newJobStore()

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/jobs", handleCreateJob)
	mux.HandleFunc("/api/jobs/", handleJobRoute)
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	go runCleanupLoop()

	addr := ":8080"
	if p := strings.TrimSpace(os.Getenv("PORT")); p != "" {
		addr = ":" + p
	}

	log.Printf("server running at http://localhost%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func runCleanupLoop() {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		jobs.cleanupExpired(time.Now())
	}
}

func handleCreateJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid form or file too large")
		return
	}

	uploadFile, header, err := getUploadFile(r)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "missing upload file")
		return
	}
	defer uploadFile.Close()

	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(header.Filename)))
	if !supportedInputExt[ext] {
		writeJSONError(w, http.StatusBadRequest, "unsupported file format, allow: pdf/txt/epub/mobi/azw3")
		return
	}

	format, err := parseFormat(r.FormValue("format"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	voice := strings.TrimSpace(r.FormValue("voice"))
	rate, err := parseRate(r.FormValue("rate"))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	workDir, err := os.MkdirTemp("", "pdftosound-job-*")
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to create temp directory")
		return
	}

	sourcePath := filepath.Join(workDir, "upload"+ext)
	if err := saveUploadedFile(uploadFile, sourcePath); err != nil {
		_ = os.RemoveAll(workDir)
		writeJSONError(w, http.StatusInternalServerError, "failed to save upload")
		return
	}

	id := newJobID()
	now := time.Now()
	base := strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))
	if strings.TrimSpace(base) == "" {
		base = "book"
	}
	job := &convertJob{
		ID:              id,
		Status:          jobQueued,
		Stage:           "任务已创建，等待处理",
		SourceName:      sanitizeFileName(base),
		Format:          format,
		Voice:           voice,
		Rate:            rate,
		CreatedAt:       now,
		UpdatedAt:       now,
		WorkDir:         workDir,
		SourcePath:      sourcePath,
		SourceExt:       ext,
		cleanupDeadline: now.Add(jobTTL),
	}
	jobs.set(job)

	go processJob(id)

	writeJSON(w, http.StatusAccepted, map[string]any{
		"job_id":       id,
		"status":       job.Status,
		"progress_url": "/api/jobs/" + id,
		"download_url": "/api/jobs/" + id + "/download",
	})
}

func handleJobRoute(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/jobs/")
	path = strings.Trim(path, "/")
	if path == "" {
		writeJSONError(w, http.StatusNotFound, "job id is required")
		return
	}

	if strings.HasSuffix(path, "/download") {
		id := strings.TrimSuffix(path, "/download")
		id = strings.Trim(id, "/")
		handleDownload(w, r, id)
		return
	}
	handleJobStatus(w, r, path)
}

func handleJobStatus(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}

	job, ok := jobs.snapshot(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}

	if job.processingStart.IsZero() {
		job.ElapsedSec = 0
	} else {
		job.ElapsedSec = int64(time.Since(job.processingStart).Seconds())
	}
	if job.Status == jobDone {
		job.DownloadURL = "/api/jobs/" + job.ID + "/download"
	}
	writeJSON(w, http.StatusOK, job)
}

func handleDownload(w http.ResponseWriter, r *http.Request, id string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}

	job, ok := jobs.snapshot(id)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "job not found")
		return
	}
	if job.Status != jobDone || job.ZipPath == "" {
		writeJSONError(w, http.StatusConflict, "job is not finished yet")
		return
	}

	if _, err := os.Stat(job.ZipPath); err != nil {
		writeJSONError(w, http.StatusNotFound, "result file not found")
		return
	}

	filename := fmt.Sprintf("%s_chapters.zip", sanitizeFileName(job.SourceName))
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	http.ServeFile(w, r, job.ZipPath)
}

func processJob(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
	defer cancel()

	_ = jobs.update(id, func(j *convertJob) {
		j.Status = jobRunning
		j.Stage = "正在解析文档"
		j.processingStart = time.Now()
		j.Progress = 0.02
	})

	job, ok := jobs.snapshot(id)
	if !ok {
		return
	}

	rawText, err := extractTextFromSource(ctx, job.SourcePath, job.SourceExt)
	if err != nil {
		if errors.Is(err, errNeedEbookConvert) {
			failJob(id, "need calibre ebook-convert for this format (epub/mobi/azw3)")
			return
		}
		failJob(id, "failed to read document text: "+err.Error())
		return
	}
	cleanText := normalizeText(rawText)
	if cleanText == "" {
		failJob(id, "pdf has no readable text")
		return
	}
	if runeCount(cleanText) > maxPDFTextRuneSize {
		cleanText = trimByRunes(cleanText, maxPDFTextRuneSize)
	}

	chapters := splitChapters(cleanText)
	if len(chapters) == 0 {
		failJob(id, "no valid chapter content found")
		return
	}
	totalRunes := 0
	for _, ch := range chapters {
		totalRunes += runeCount(ch.Text)
	}
	estimateTotalSec := estimateTotalSeconds(totalRunes, len(chapters), job.Rate, job.Format)

	_ = jobs.update(id, func(j *convertJob) {
		j.TotalChapters = len(chapters)
		j.DoneChapters = 0
		j.totalRunes = totalRunes
		j.EstimatedTotalSec = estimateTotalSec
		j.EstimatedRemainSec = estimateTotalSec
		j.Stage = "开始生成章节音频"
		j.Progress = 0.05
	})

	audioFiles := make([]string, 0, len(chapters))
	chapterDurations := make([]time.Duration, 0, len(chapters))
	for i, ch := range chapters {
		chText := strings.TrimSpace(ch.Text)
		if chText == "" {
			continue
		}

		start := time.Now()
		_ = jobs.update(id, func(j *convertJob) {
			j.Stage = "正在生成音频"
			j.CurrentChapter = ch.Title
		})

		outPath := filepath.Join(job.WorkDir, fmt.Sprintf("%02d_%s.%s", i+1, sanitizeFileName(ch.Title), job.Format))
		if err := synthesizeChapter(ctx, chText, outPath, job.Format, job.Voice, job.Rate); err != nil {
			if errors.Is(err, errNeedFFmpeg) {
				failJob(id, "mp3 requires ffmpeg installed in PATH")
				return
			}
			if errors.Is(err, errUnsupportedAudioFormat) {
				failJob(id, "unsupported format on current OS")
				return
			}
			failJob(id, "failed to synthesize audio: "+err.Error())
			return
		}
		audioFiles = append(audioFiles, outPath)

		chapterDurations = append(chapterDurations, time.Since(start))
		done := len(audioFiles)
		_ = jobs.update(id, func(j *convertJob) {
			j.DoneChapters = done
			j.Progress = 0.05 + 0.85*float64(done)/float64(maxInt(1, len(chapters)))
			j.EstimatedRemainSec = estimateRemainingSeconds(chapterDurations, done, len(chapters))
		})
	}

	if len(audioFiles) == 0 {
		failJob(id, "no chapter audio generated")
		return
	}

	_ = jobs.update(id, func(j *convertJob) {
		j.Stage = "正在打包 ZIP"
		j.Progress = 0.93
	})

	zipPath := filepath.Join(job.WorkDir, fmt.Sprintf("%s_chapters.zip", sanitizeFileName(job.SourceName)))
	if err := zipFilesToDisk(audioFiles, zipPath); err != nil {
		failJob(id, "failed to package zip: "+err.Error())
		return
	}

	_ = jobs.update(id, func(j *convertJob) {
		j.ZipPath = zipPath
		j.Status = jobDone
		j.Stage = "处理完成"
		j.CurrentChapter = ""
		j.Progress = 1
		j.EstimatedRemainSec = 0
		j.DownloadURL = "/api/jobs/" + j.ID + "/download"
		j.cleanupDeadline = time.Now().Add(jobTTL)
	})
}

func failJob(id, msg string) {
	_ = jobs.update(id, func(j *convertJob) {
		j.Status = jobFailed
		j.Stage = "处理失败"
		j.Error = msg
		j.cleanupDeadline = time.Now().Add(jobTTL)
	})
}

func estimateTotalSeconds(totalRunes, totalChapters, rate int, format string) int64 {
	if rate <= 0 {
		rate = 180
	}
	words := float64(totalRunes) / 1.6
	speechSec := words * 60.0 / float64(rate)
	overhead := float64(totalChapters*6 + 6)
	if format == "mp3" {
		overhead += float64(totalChapters * 3)
	}
	total := int64(speechSec + overhead)
	if total < 15 {
		total = 15
	}
	return total
}

func estimateRemainingSeconds(durations []time.Duration, done, total int) int64 {
	if done <= 0 || total <= done {
		return 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	avg := sum / time.Duration(done)
	remain := avg * time.Duration(total-done)
	sec := int64(remain.Seconds())
	if sec < 1 {
		sec = 1
	}
	return sec
}

func parseFormat(input string) (string, error) {
	format := strings.ToLower(strings.TrimSpace(input))
	if format == "" {
		format = "mp3"
	}
	if format == "mp4" {
		format = "m4a"
	}
	if format != "mp3" && format != "m4a" {
		return "", errors.New("format must be mp3, mp4, or m4a")
	}
	return format, nil
}

func parseRate(input string) (int, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return 180, nil
	}
	rate, err := strconv.Atoi(input)
	if err != nil {
		return 0, errors.New("rate must be a number")
	}
	if rate < 90 || rate > 360 {
		return 0, errors.New("rate must be between 90 and 360")
	}
	return rate, nil
}

func getUploadFile(r *http.Request) (multipartFile, *multipart.FileHeader, error) {
	if f, h, err := r.FormFile("file"); err == nil {
		return f, h, nil
	}
	return r.FormFile("pdf")
}

func saveUploadedFile(src multipartFile, dstPath string) error {
	dst, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dst.Close()
	_, err = io.Copy(dst, src)
	return err
}

type multipartFile interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
}

func extractPDFText(pdfPath string) (string, error) {
	f, reader, err := pdf.Open(pdfPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	var b strings.Builder
	totalPage := reader.NumPage()
	for i := 1; i <= totalPage; i++ {
		p := reader.Page(i)
		if p.V.IsNull() {
			continue
		}
		text, err := p.GetPlainText(nil)
		if err != nil {
			continue
		}
		if strings.TrimSpace(text) == "" {
			continue
		}
		b.WriteString(text)
		b.WriteString("\n\n")
	}

	return b.String(), nil
}

func extractTextFromSource(ctx context.Context, path, ext string) (string, error) {
	switch strings.ToLower(ext) {
	case ".pdf":
		return extractPDFText(path)
	case ".txt":
		return extractTextFile(path)
	case ".epub", ".mobi", ".azw3", ".azw":
		return extractTextViaEbookConvert(ctx, path)
	default:
		return "", fmt.Errorf("unsupported source format: %s", ext)
	}
}

func extractTextFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func extractTextViaEbookConvert(ctx context.Context, sourcePath string) (string, error) {
	if !commandExists("ebook-convert") {
		return "", errNeedEbookConvert
	}

	outPath := sourcePath + ".converted.txt"
	if err := runCommand(ctx, "ebook-convert", sourcePath, outPath); err != nil {
		return "", err
	}
	defer os.Remove(outPath)

	return extractTextFile(outPath)
}

func normalizeText(s string) string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	lines := strings.Split(s, "\n")
	paragraphs := make([]string, 0, len(lines)/2)
	mergedLines := make([]string, 0, 32)

	flushParagraph := func() {
		if len(mergedLines) == 0 {
			return
		}
		paragraphs = append(paragraphs, joinLinesForSpeech(mergedLines))
		mergedLines = mergedLines[:0]
	}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			flushParagraph()
			continue
		}
		if isChapterHeading(line) {
			flushParagraph()
			paragraphs = append(paragraphs, line)
			continue
		}
		line = compactSpacesPreserveCJK(strings.Join(strings.Fields(line), " "))
		if line == "" {
			continue
		}
		mergedLines = append(mergedLines, line)
	}
	flushParagraph()
	return strings.TrimSpace(strings.Join(paragraphs, "\n\n"))
}

func splitChapters(text string) []chapter {
	lines := strings.Split(text, "\n")
	var chapters []chapter
	currentTitle := "前言"
	var buf strings.Builder

	flush := func() {
		content := strings.TrimSpace(buf.String())
		if content == "" {
			return
		}
		chapters = append(chapters, chapter{Title: currentTitle, Text: content})
		buf.Reset()
	}

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			buf.WriteString("\n")
			continue
		}
		if isChapterHeading(trimmed) {
			flush()
			currentTitle = trimmed
			continue
		}
		buf.WriteString(trimmed)
		buf.WriteString("\n")
	}
	flush()

	if len(chapters) > 0 {
		return splitLargeChapters(chapters, 6000)
	}
	return chunkByLength(text, 6000)
}

func isChapterHeading(line string) bool {
	for _, re := range chapterRegexes {
		if re.MatchString(line) {
			return true
		}
	}
	return false
}

func chunkByLength(text string, maxRunes int) []chapter {
	parts := chunkTextBySentences(text, maxRunes)
	if len(parts) == 0 {
		return nil
	}
	out := make([]chapter, 0, len(parts))
	for i, part := range parts {
		out = append(out, chapter{
			Title: fmt.Sprintf("Part_%02d", i+1),
			Text:  part,
		})
	}
	return out
}

func synthesizeChapter(ctx context.Context, text, outPath, format, voice string, rate int) error {
	switch runtime.GOOS {
	case "darwin":
		return synthesizeDarwin(ctx, text, outPath, format, voice, rate)
	case "linux":
		return synthesizeLinux(ctx, text, outPath, format, voice, rate)
	default:
		return fmt.Errorf("unsupported OS: %s", runtime.GOOS)
	}
}

func synthesizeDarwin(ctx context.Context, text, outPath, format, voice string, rate int) error {
	textPath := outPath + ".txt"
	if err := os.WriteFile(textPath, []byte(text), 0644); err != nil {
		return err
	}
	defer os.Remove(textPath)

	switch format {
	case "m4a":
		args := []string{}
		if voice != "" {
			args = append(args, "-v", voice)
		}
		args = append(args, "-r", strconv.Itoa(rate), "-f", textPath, "-o", outPath)
		return runCommand(ctx, "say", args...)
	case "mp3":
		if !commandExists("ffmpeg") {
			return errNeedFFmpeg
		}
		tmpM4A := outPath + ".tmp.m4a"
		args := []string{}
		if voice != "" {
			args = append(args, "-v", voice)
		}
		args = append(args, "-r", strconv.Itoa(rate), "-f", textPath, "-o", tmpM4A)
		if err := runCommand(ctx, "say", args...); err != nil {
			return err
		}
		defer os.Remove(tmpM4A)
		return runCommand(ctx, "ffmpeg", "-y", "-i", tmpM4A, "-codec:a", "libmp3lame", "-q:a", "2", outPath)
	default:
		return errUnsupportedAudioFormat
	}
}

func synthesizeLinux(ctx context.Context, text, outPath, format, voice string, rate int) error {
	engine := ""
	if commandExists("espeak-ng") {
		engine = "espeak-ng"
	} else if commandExists("espeak") {
		engine = "espeak"
	} else {
		return errors.New("espeak-ng/espeak not found")
	}

	tmpWav := outPath + ".tmp.wav"
	args := []string{"-w", tmpWav, "-s", strconv.Itoa(rate)}
	if voice != "" {
		args = append(args, "-v", voice)
	}
	args = append(args, text)
	if err := runCommand(ctx, engine, args...); err != nil {
		return err
	}
	defer os.Remove(tmpWav)

	switch format {
	case "mp3":
		if !commandExists("ffmpeg") {
			return errNeedFFmpeg
		}
		return runCommand(ctx, "ffmpeg", "-y", "-i", tmpWav, "-codec:a", "libmp3lame", "-q:a", "2", outPath)
	case "m4a":
		if !commandExists("ffmpeg") {
			return errNeedFFmpeg
		}
		return runCommand(ctx, "ffmpeg", "-y", "-i", tmpWav, "-codec:a", "aac", "-b:a", "128k", outPath)
	default:
		return errUnsupportedAudioFormat
	}
}

func splitLargeChapters(chapters []chapter, maxRunes int) []chapter {
	if maxRunes <= 0 {
		maxRunes = 6000
	}
	out := make([]chapter, 0, len(chapters))
	for _, ch := range chapters {
		parts := chunkTextBySentences(ch.Text, maxRunes)
		if len(parts) <= 1 {
			out = append(out, chapter{
				Title: ch.Title,
				Text:  prepareSpeechText(ch.Text),
			})
			continue
		}
		for i, part := range parts {
			out = append(out, chapter{
				Title: fmt.Sprintf("%s_%02d", ch.Title, i+1),
				Text:  part,
			})
		}
	}
	return out
}

func chunkTextBySentences(text string, maxRunes int) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return nil
	}
	if maxRunes <= 0 {
		maxRunes = 6000
	}

	sentences := splitSentences(text)
	if len(sentences) == 0 {
		return hardChunkByRunes(text, maxRunes)
	}

	out := make([]string, 0, maxInt(1, len(sentences)/3))
	var b strings.Builder
	currentRunes := 0

	flush := func() {
		chunk := strings.TrimSpace(b.String())
		if chunk != "" {
			out = append(out, chunk)
		}
		b.Reset()
		currentRunes = 0
	}

	for _, sentence := range sentences {
		sentence = strings.TrimSpace(sentence)
		if sentence == "" {
			continue
		}
		rs := runeCount(sentence)
		if rs > maxRunes {
			flush()
			out = append(out, splitLongSentence(sentence, maxRunes)...)
			continue
		}

		extra := rs
		if currentRunes > 0 {
			extra++ // newline separator between sentences
		}
		if currentRunes > 0 && currentRunes+extra > maxRunes {
			flush()
		}
		if currentRunes > 0 {
			b.WriteByte('\n')
			currentRunes++
		}
		b.WriteString(sentence)
		currentRunes += rs
	}
	flush()

	if len(out) == 0 {
		return hardChunkByRunes(text, maxRunes)
	}
	return out
}

func splitLongSentence(sentence string, maxRunes int) []string {
	rs := []rune(strings.TrimSpace(sentence))
	if len(rs) == 0 {
		return nil
	}
	if len(rs) <= maxRunes {
		return []string{string(rs)}
	}

	clauses := make([]string, 0, 8)
	start := 0
	for i, r := range rs {
		if !isClauseBreakRune(r) {
			continue
		}
		part := strings.TrimSpace(string(rs[start : i+1]))
		if part != "" {
			clauses = append(clauses, part)
		}
		start = i + 1
	}
	if start < len(rs) {
		part := strings.TrimSpace(string(rs[start:]))
		if part != "" {
			clauses = append(clauses, part)
		}
	}
	if len(clauses) <= 1 {
		return hardChunkByRunes(sentence, maxRunes)
	}

	out := make([]string, 0, len(clauses))
	var b strings.Builder
	currentRunes := 0
	flush := func() {
		chunk := strings.TrimSpace(b.String())
		if chunk != "" {
			out = append(out, chunk)
		}
		b.Reset()
		currentRunes = 0
	}

	for _, clause := range clauses {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			continue
		}
		rsLen := runeCount(clause)
		if rsLen > maxRunes {
			flush()
			out = append(out, hardChunkByRunes(clause, maxRunes)...)
			continue
		}
		extra := rsLen
		if currentRunes > 0 {
			extra++
		}
		if currentRunes > 0 && currentRunes+extra > maxRunes {
			flush()
		}
		if currentRunes > 0 {
			b.WriteByte('\n')
			currentRunes++
		}
		b.WriteString(clause)
		currentRunes += rsLen
	}
	flush()
	return out
}

func hardChunkByRunes(text string, maxRunes int) []string {
	rs := []rune(strings.TrimSpace(text))
	if len(rs) == 0 {
		return nil
	}
	if maxRunes <= 0 {
		maxRunes = 6000
	}

	out := make([]string, 0, (len(rs)+maxRunes-1)/maxRunes)
	for start := 0; start < len(rs); {
		end := start + maxRunes
		if end > len(rs) {
			end = len(rs)
		}
		out = append(out, strings.TrimSpace(string(rs[start:end])))
		start = end
	}
	return out
}

func prepareSpeechText(text string) string {
	sentences := splitSentences(text)
	if len(sentences) == 0 {
		return strings.TrimSpace(text)
	}
	return strings.Join(sentences, "\n")
}

func splitSentences(text string) []string {
	rs := []rune(strings.TrimSpace(text))
	if len(rs) == 0 {
		return nil
	}
	out := make([]string, 0, len(rs)/20+1)
	start := 0

	appendSegment := func(seg string) {
		seg = compactSpacesPreserveCJK(strings.Join(strings.Fields(strings.TrimSpace(seg)), " "))
		if seg == "" {
			return
		}
		out = append(out, ensureSentenceEnding(seg))
	}

	for i := 0; i < len(rs); i++ {
		r := rs[i]
		if r == '\n' && i+1 < len(rs) && rs[i+1] == '\n' {
			appendSegment(string(rs[start:i]))
			start = i + 2
			i++
			continue
		}
		if !isSentenceTerminalRune(r) {
			continue
		}
		end := i + 1
		for end < len(rs) && isSentenceCloserRune(rs[end]) {
			end++
		}
		appendSegment(string(rs[start:end]))
		start = end
		i = end - 1
	}
	if start < len(rs) {
		appendSegment(string(rs[start:]))
	}
	return out
}

func joinLinesForSpeech(lines []string) string {
	if len(lines) == 0 {
		return ""
	}
	var b strings.Builder
	var prevLast rune
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i > 0 && b.Len() > 0 {
			first, _ := firstRune(line)
			if !shouldJoinWithoutSpace(prevLast, first) {
				b.WriteByte(' ')
			}
		}
		b.WriteString(line)
		prevLast, _ = lastRune(line)
	}
	return compactSpacesPreserveCJK(strings.TrimSpace(b.String()))
}

func compactSpacesPreserveCJK(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	for i := 0; i < 6; i++ {
		before := text
		text = reHanSpaceHan.ReplaceAllString(text, "$1$2")
		text = reHanSpaceClosePunct.ReplaceAllString(text, "$1$2")
		text = reOpenPunctSpaceHan.ReplaceAllString(text, "$1$2")
		if text == before {
			break
		}
	}
	return text
}

func ensureSentenceEnding(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return s
	}
	r, ok := lastRune(s)
	if !ok {
		return s
	}
	if isSentenceTerminalRune(r) || isSentenceCloserRune(r) {
		return s
	}
	if containsCJK(s) {
		return s + "。"
	}
	return s + "."
}

func containsCJK(s string) bool {
	for _, r := range s {
		if isCJKRune(r) {
			return true
		}
	}
	return false
}

func shouldJoinWithoutSpace(left, right rune) bool {
	if left == 0 || right == 0 {
		return false
	}
	if isCJKRune(left) && isCJKRune(right) {
		return true
	}
	if isCJKRune(left) && isCJKPunctuation(right) {
		return true
	}
	if isCJKPunctuation(left) && isCJKRune(right) {
		return true
	}
	return false
}

func isSentenceTerminalRune(r rune) bool {
	switch r {
	case '.', '!', '?', ';', '。', '！', '？', '；', '…':
		return true
	default:
		return false
	}
}

func isSentenceCloserRune(r rune) bool {
	switch r {
	case ')', ']', '}', '"', '\'', '）', '】', '》', '”', '’':
		return true
	default:
		return false
	}
}

func isClauseBreakRune(r rune) bool {
	switch r {
	case ',', '，', '、', ':', '：':
		return true
	default:
		return false
	}
}

func isCJKPunctuation(r rune) bool {
	switch r {
	case '，', '。', '！', '？', '；', '：', '、', '（', '）', '《', '》', '【', '】', '“', '”', '‘', '’':
		return true
	default:
		return false
	}
}

func isCJKRune(r rune) bool {
	return unicode.In(r, unicode.Han)
}

func firstRune(s string) (rune, bool) {
	for _, r := range s {
		return r, true
	}
	return 0, false
}

func lastRune(s string) (rune, bool) {
	rs := []rune(s)
	if len(rs) == 0 {
		return 0, false
	}
	return rs[len(rs)-1], true
}

func zipFilesToDisk(paths []string, zipPath string) error {
	f, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer f.Close()

	zw := zip.NewWriter(f)
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			_ = zw.Close()
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			_ = zw.Close()
			return err
		}
		header.Name = filepath.Base(p)
		header.Method = zip.Deflate

		w, err := zw.CreateHeader(header)
		if err != nil {
			_ = zw.Close()
			return err
		}
		src, err := os.Open(p)
		if err != nil {
			_ = zw.Close()
			return err
		}
		_, copyErr := io.Copy(w, src)
		closeErr := src.Close()
		if copyErr != nil {
			_ = zw.Close()
			return copyErr
		}
		if closeErr != nil {
			_ = zw.Close()
			return closeErr
		}
	}
	return zw.Close()
}

func runCommand(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		return fmt.Errorf("%s %v failed: %s", name, args, msg)
	}
	return nil
}

func commandExists(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

func sanitizeFileName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "chapter"
	}
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_' || r == '-' {
			b.WriteRune(r)
			continue
		}
		if unicode.IsSpace(r) {
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "chapter"
	}
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}

func runeCount(s string) int {
	return len([]rune(s))
}

func trimByRunes(s string, max int) string {
	rs := []rune(s)
	if len(rs) <= max {
		return s
	}
	return string(rs[:max])
}

func newJobID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("job_%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
