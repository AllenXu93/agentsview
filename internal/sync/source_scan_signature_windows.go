//go:build windows

package sync

import (
	"os"

	"golang.org/x/sys/windows"
)

// Directory enumeration can report stale size and write time while a writer
// holds the file open. Every signature field comes from one fresh handle.
func sourceScanFileSignature(path string, _ os.DirEntry) (sourceScanSignature, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return sourceScanSignature{}, false, err
	}
	defer file.Close() // Read-only metadata handle.
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &info); err != nil {
		return sourceScanSignature{}, false, &os.PathError{Op: "GetFileInformationByHandle", Path: path, Err: err}
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_DIRECTORY != 0 {
		return sourceScanSignature{}, false, nil
	}
	return sourceScanSignature{
		size:   int64(info.FileSizeHigh)<<32 | int64(info.FileSizeLow),
		mtime:  info.LastWriteTime.Nanoseconds(),
		inode:  int64(uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)),
		device: int64(info.VolumeSerialNumber),
	}, true, nil
}
