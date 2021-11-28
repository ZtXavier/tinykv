package standalone_storage

import (
  "path/filepath"
  "github.com/Connor1996/badger"
  "github.com/pingcap-incubator/tinykv/kv/config"
  "github.com/pingcap-incubator/tinykv/kv/storage"
  "github.com/pingcap-incubator/tinykv/kv/util/engine_util"
  "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of Storage for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
  // Your Data Here (1).
  engine    *engine_util.Engines
  conf    *config.Config
  Txn       *badger.Txn
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
  // Your Code Here (1).
  kvPath := filepath.Join(conf.DBPath, "kv")
  raftPath := filepath.Join(conf.DBPath, "raft")
  raftDB := engine_util.CreateDB(raftPath, true)
  kvDB := engine_util.CreateDB(kvPath, false)
  engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
  return &StandAloneStorage {
    engine:  engines,
    conf:       conf,
  }
}

func (s *StandAloneStorage) Start() error {
  // Your Code Here (1).
  return nil
}

func (s *StandAloneStorage) Stop() error {
  // Your Code Here (1).
  return s.engine.Close()
}

//只读事务的实现
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
  // Your Code Here (1).
  txn := s.engine.Kv.NewTransaction(false)
  return &StandAloneStorage{
    Txn : txn,
  }, nil
}

func (s *StandAloneStorage) GetCF(cf string,key []byte) ([]byte,error){
  iter, err := engine_util.GetCFFromTxn(s.Txn,cf,key)
  if err == badger.ErrKeyNotFound{
    return nil,nil
  }
  return iter, err
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator{
  return engine_util.NewCFIterator(cf, s.Txn)
}

func (s *StandAloneStorage) Close() {
  s.Txn.Discard()
}


func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
  // Your Code Here (1).
  for _, tp := range batch{
    switch tp.Data.(type){
    case storage.Put:
      rerr := engine_util.PutCF(s.engine.Kv,tp.Cf(),tp.Key(),tp.Value())
      if rerr != nil {
        return rerr
      }
    case storage.Delete:
      rerr := engine_util.DeleteCF(s.engine.Kv,tp.Cf(),tp.Key())
      if rerr != nil {
        return rerr
      }
    }
  }
  return nil
}