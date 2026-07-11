package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// replayRecordsNoCRC 是**故意有 bug** 的负向变体：跳过 crc 校验，只看长度。
// 它的存在只为证明 replayRecords 里的 crc 校验是承重的——位翻转的 payload 必须被正确
// 实现拦下、被这个变体漏过。若测试对两者结果无法区分，说明测试没覆盖到 crc 契约。
func replayRecordsNoCRC(data []byte) (records [][]byte, validLen int) {
	off := 0
	for off+recordHeaderSize <= len(data) {
		n := int(binary.BigEndian.Uint32(data[off : off+4]))
		if n < 0 || n > len(data)-off-recordHeaderSize {
			break
		}
		payload := data[off+recordHeaderSize : off+recordHeaderSize+n]
		rec := make([]byte, n)
		copy(rec, payload)
		records = append(records, rec)
		off += recordHeaderSize + n
		validLen = off
	}
	return records, validLen
}

func buildLog(payloads ...[]byte) []byte {
	var data []byte
	for _, p := range payloads {
		data = appendRecord(data, p)
	}
	return data
}

func assertRecords(t *testing.T, got [][]byte, want ...[]byte) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("record count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Fatalf("record %d: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestFramingRoundTrip(t *testing.T) {
	a, b, c := []byte("alpha"), []byte(""), []byte("charlie")
	data := buildLog(a, b, c)
	recs, validLen := replayRecords(data)
	assertRecords(t, recs, a, b, c)
	if validLen != len(data) {
		t.Fatalf("validLen: got %d, want %d (whole buffer valid)", validLen, len(data))
	}
}

func TestReplayTornHeader(t *testing.T) {
	a := []byte("alpha")
	good := buildLog(a)
	// 尾部加 3 字节残缺头（< recordHeaderSize）——模拟头没写全。
	data := append(append([]byte{}, good...), 0x01, 0x02, 0x03)
	recs, validLen := replayRecords(data)
	assertRecords(t, recs, a)
	if validLen != len(good) {
		t.Fatalf("validLen: got %d, want %d (drop torn header)", validLen, len(good))
	}
}

func TestReplayTornPayload(t *testing.T) {
	a := []byte("alpha")
	data := buildLog(a)
	// 手写一条头：声称 payload 100 字节，实际只跟 4 字节 → len 越界。
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], 100)
	binary.BigEndian.PutUint32(hdr[4:8], 0xdeadbeef)
	data = append(data, hdr[:]...)
	data = append(data, 0x01, 0x02, 0x03, 0x04)
	recs, validLen := replayRecords(data)
	assertRecords(t, recs, a)
	if validLen != len(buildLog(a)) {
		t.Fatalf("validLen: got %d, want %d (drop torn payload)", validLen, len(buildLog(a)))
	}
}

// TestReplayBitFlipCRC：len 完整、但 payload 被翻转一位。只有 crc 能抓到。
func TestReplayBitFlipCRC(t *testing.T) {
	a, b := []byte("alpha"), []byte("bravo")
	data := buildLog(a, b)
	// b 的 payload 起点 = 两个头 + a 的长度。
	bPayloadOff := 2*recordHeaderSize + len(a)
	data[bPayloadOff] ^= 0xFF // 翻转 b 的首字节
	recs, validLen := replayRecords(data)
	assertRecords(t, recs, a) // b 被 crc 拦下
	if validLen != len(buildLog(a)) {
		t.Fatalf("validLen: got %d, want %d (drop crc-corrupt record)", validLen, len(buildLog(a)))
	}
	// 完整性自检：crc 字段确实与被改后的 payload 不符。
	if crc32.ChecksumIEEE(data[bPayloadOff:bPayloadOff+len(b)]) ==
		binary.BigEndian.Uint32(data[bPayloadOff-recordHeaderSize+4:bPayloadOff-recordHeaderSize+8]) {
		t.Fatal("test setup wrong: crc still matches after bitflip")
	}
}

// TestNegativeVariant_NoCRC_MissesBitFlip：同一份位翻转数据，正确实现丢掉坏 record、
// no-crc 变体漏过它。两者结果必须不同——否则 crc 校验的测试形同虚设。
func TestNegativeVariant_NoCRC_MissesBitFlip(t *testing.T) {
	a, b := []byte("alpha"), []byte("bravo")
	data := buildLog(a, b)
	data[2*recordHeaderSize+len(a)] ^= 0xFF

	good, _ := replayRecords(data)
	bad, _ := replayRecordsNoCRC(data)

	if len(good) != 1 {
		t.Fatalf("correct impl should drop corrupt record: got %d records", len(good))
	}
	if len(bad) != 2 {
		t.Fatalf("no-crc variant should MISS the corruption (keep 2): got %d records", len(bad))
	}
	if len(good) == len(bad) {
		t.Fatal("test cannot distinguish correct vs buggy impl → crc contract not covered")
	}
}

// TestRecordLogHealsTornTail：文件级——写入若干 record、关闭，往文件尾追加垃圾（模拟
// 崩溃留下的撕裂尾巴），重开时应恢复出干净的 record 并把垃圾物理截断，之后还能继续 append。
func TestRecordLogHealsTornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	a, b := []byte("alpha"), []byte("bravo")

	l, recs, err := OpenRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs) // 新文件为空
	if err := l.Append(a); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(b); err != nil {
		t.Fatal(err)
	}
	l.Close()

	cleanSize := fileSize(t, path)

	// 模拟崩溃：往文件尾追加一条半截 record（头声称很长，实际只有几字节）。
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], 9999)
	f.Write(hdr[:])
	f.Write([]byte("torn"))
	f.Close()

	// 重开：应 healing 掉撕裂尾巴。
	l2, recs2, err := OpenRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs2, a, b)
	if got := fileSize(t, path); got != cleanSize {
		t.Fatalf("file not truncated to clean size: got %d, want %d", got, cleanSize)
	}
	// healing 后还能继续正确 append。
	c := []byte("charlie")
	if err := l2.Append(c); err != nil {
		t.Fatal(err)
	}
	l2.Close()

	_, recs3, err := OpenRecordLog(path)
	if err != nil {
		t.Fatal(err)
	}
	assertRecords(t, recs3, a, b, c)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
