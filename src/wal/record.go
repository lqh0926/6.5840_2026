// Package wal 实现崩溃安全的持久化原语（Phase 2 Step 2/3）。
//
// 本文件是「尾部追加」写模式的原语：append-only 的 framed record log。崩溃安全**不是**
// 靠把单次 write+fsync 做成原子（崩溃能砸在 write 中间，挡不住），而是靠**每条 record
// 自描述 + 自校验**：replay 时撞到 len 越界或 crc 不符即判定 torn tail，在最后一条完整
// record 处截断。append-only 下崩溃只会砸没落盘的尾巴，已 fsync 的老 record 不受影响。
package wal

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

// recordHeaderSize 是每条 record 的定长头：[len:u32][crc32:u32]（均大端）。
const recordHeaderSize = 8

// appendRecord 把 payload 编码成 [len][crc32(payload)][payload] 追加到 dst。
func appendRecord(dst, payload []byte) []byte {
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))
	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// replayRecords 顺序解析 data 中的 record，撞到第一条不完整/损坏的（torn tail）即停。
// 返回解码出的 payload 列表，以及 validLen —— 构成完整且 crc 合法 record 的前缀字节数，
// 也就是「文件应截断到的位置」（healing 掉撕裂的尾巴）。
//
// 两类 torn 都在此拦下：① len 声称的 payload 比剩余字节多（半截写/头没写全）；
// ② len 完整但 payload 被改（位翻转）→ crc 不符。前者靠边界检查，后者靠 crc——只靠 len
// 挡不住"长度对得上但内容坏了"的情况，这正是 crc 存在的理由。
func replayRecords(data []byte) (records [][]byte, validLen int) {
	off := 0
	for off+recordHeaderSize <= len(data) {
		n := int(binary.BigEndian.Uint32(data[off : off+4]))
		crc := binary.BigEndian.Uint32(data[off+4 : off+8])
		if n < 0 || n > len(data)-off-recordHeaderSize {
			break // torn：len 越界（含头没写全导致的巨大/负长度）
		}
		payload := data[off+recordHeaderSize : off+recordHeaderSize+n]
		if crc32.ChecksumIEEE(payload) != crc {
			break // 损坏：payload 被改（len 对得上但内容坏了）
		}
		rec := make([]byte, n)
		copy(rec, payload)
		records = append(records, rec)
		off += recordHeaderSize + n
		validLen = off
	}
	return records, validLen
}

// RecordLog 是文件背书的崩溃安全 append-only record log。
//   - Append：追加一条 record 后 **fsync 才返回**（回 ack 前须 durable）。
//   - Open：读回文件、replay，若尾部撕裂则物理 Truncate 掉，使后续 append 从干净处开始。
type RecordLog struct {
	f    *os.File
	size int64 // 当前有效字节长度（= 下一条 append 的写入位置）
}

// OpenRecordLog 打开（或创建）path 处的日志，healing 掉任何撕裂尾巴，返回恢复出的
// record 列表和一个定位在有效末尾、可继续 append 的 RecordLog。
func OpenRecordLog(path string) (*RecordLog, [][]byte, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	records, validLen := replayRecords(data)
	if validLen != len(data) {
		// 撕裂尾巴：物理截断到最后一条完整 record。
		if err := f.Truncate(int64(validLen)); err != nil {
			f.Close()
			return nil, nil, err
		}
	}
	if _, err := f.Seek(int64(validLen), io.SeekStart); err != nil {
		f.Close()
		return nil, nil, err
	}
	return &RecordLog{f: f, size: int64(validLen)}, records, nil
}

// Append 编码并写入 payload，fsync 成功后才返回（返回即 durable）。
func (l *RecordLog) Append(payload []byte) error {
	rec := appendRecord(nil, payload)
	if _, err := l.f.Write(rec); err != nil {
		return err
	}
	if err := l.f.Sync(); err != nil {
		return err
	}
	l.size += int64(len(rec))
	return nil
}

// Close 关闭底层文件。
func (l *RecordLog) Close() error { return l.f.Close() }
