package persist

import (
	"bytes"
	"testing"
)

func TestFilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	rs := []byte("raft-state-blob")
	snap := []byte("snapshot-blob")
	p.Save(rs, snap)

	if got := p.ReadRaftState(); !bytes.Equal(got, rs) {
		t.Errorf("ReadRaftState = %q, want %q", got, rs)
	}
	if got := p.ReadSnapshot(); !bytes.Equal(got, snap) {
		t.Errorf("ReadSnapshot = %q, want %q", got, snap)
	}
	if got := p.RaftStateSize(); got != len(rs) {
		t.Errorf("RaftStateSize = %d, want %d", got, len(rs))
	}
}

// TestFilePersisterReopen 模拟进程崩溃重启：重新 Open 同一目录应读回上次 Save 的状态。
func TestFilePersisterReopen(t *testing.T) {
	dir := t.TempDir()

	p1, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	rs := []byte{0x01, 0x02, 0x00, 0xff} // 含 0 字节，验证长度前缀而非以 0 截断
	snap := []byte("SNAP")
	p1.Save(rs, snap)

	// 全新实例，只能从磁盘恢复
	p2, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := p2.ReadRaftState(); !bytes.Equal(got, rs) {
		t.Errorf("reopen ReadRaftState = %v, want %v", got, rs)
	}
	if got := p2.ReadSnapshot(); !bytes.Equal(got, snap) {
		t.Errorf("reopen ReadSnapshot = %q, want %q", got, snap)
	}
}

func TestFilePersisterEmpty(t *testing.T) {
	dir := t.TempDir()
	p, err := OpenFilePersister(dir)
	if err != nil {
		t.Fatal(err)
	}
	// 从未 Save：读应为 nil，Size 为 0
	if got := p.ReadRaftState(); got != nil {
		t.Errorf("empty ReadRaftState = %v, want nil", got)
	}
	if got := p.ReadSnapshot(); got != nil {
		t.Errorf("empty ReadSnapshot = %v, want nil", got)
	}
	if got := p.RaftStateSize(); got != 0 {
		t.Errorf("empty RaftStateSize = %d, want 0", got)
	}

	// 只存 raftstate、snapshot 为 nil
	p.Save([]byte("only-state"), nil)
	if got := p.ReadSnapshot(); got != nil {
		t.Errorf("nil snapshot round-trips to %v, want nil", got)
	}
	if got := p.ReadRaftState(); !bytes.Equal(got, []byte("only-state")) {
		t.Errorf("ReadRaftState = %q", got)
	}
}

// TestFilePersisterOverwrite 验证多次 Save 后读到的是最后一次。
func TestFilePersisterOverwrite(t *testing.T) {
	dir := t.TempDir()
	p, _ := OpenFilePersister(dir)
	for i := 0; i < 5; i++ {
		p.Save([]byte{byte(i)}, []byte{byte(i * 10)})
	}
	p2, _ := OpenFilePersister(dir)
	if got := p2.ReadRaftState(); !bytes.Equal(got, []byte{4}) {
		t.Errorf("after overwrite ReadRaftState = %v, want [4]", got)
	}
	if got := p2.ReadSnapshot(); !bytes.Equal(got, []byte{40}) {
		t.Errorf("after overwrite ReadSnapshot = %v, want [40]", got)
	}
}

// 编译期确认 FilePersister 满足 Persister 接口。
var _ Persister = (*FilePersister)(nil)
