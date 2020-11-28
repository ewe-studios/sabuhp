package utils

import (
	"mime"
	"path/filepath"
)

var (
	mediaTypes = map[string]string{
		".txt":      "text/plain",
		".text":     "text/plain",
		".html":     "text/html",
		".css":      "text/css",
		".js":       "text/javascript",
		".erb":      "template/erb",
		".min.css":  "text/css",
		".haml":     "text/haml",
		".markdown": "text/markdown",
		".md":       "text/markdown",
		".svg":      "image/svg+xml",
		".png":      "image/png",
		".jpg":      "image/jpg",
		".gif":      "image/png",
		".mp3":      "audio/mp3",
	}
)

// GetFileMimeType returns associated mime type for giving file extension.
func GetFileMimeType(path string) string {
	ext := filepath.Ext(path)
	extVal := mime.TypeByExtension(ext)
	if extVal == "" {
		extVal = mediaTypes[ext]
	}
	return extVal
}
