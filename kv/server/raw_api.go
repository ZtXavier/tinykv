package server

import (
  "context"

  "github.com/pingcap-incubator/tinykv/kv/storage"
  "github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
  // Your Code Here (1).
  get, _ := server.storage.Reader(nil)

  v, err := get.GetCF(req.Cf,req.Key)

  if err != nil{
    return &kvrpcpb.RawGetResponse{
      NotFound: true,
    },err
  }
  if err == nil{
    return &kvrpcpb.RawGetResponse{
      NotFound: true,
      Value: v,
    }, nil
  }
  return nil, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
  // Your Code Here (1).
  // Hint: Consider using Storage.Modify to store data to be modified
  err := server.storage.Write(nil,[]storage.Modify{
    {
      Data: storage.Put{
        Cf:    req.Cf,
        Key:   req.Key,
        Value: req.Value,
      },
    },
  })
  if err == nil{
    return &kvrpcpb.RawPutResponse{
    }, err
  }
  return &kvrpcpb.RawPutResponse{
  }, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
  // Your Code Here (1).
  // Hint: Consider using Storage.Modify to store data to be deleted
  erro := server.storage.Write(nil,[]storage.Modify{
    {
      Data: storage.Delete{
        Cf:    req.Cf,
        Key:   req.Key,
      },
    },
  })
  return &kvrpcpb.RawDeleteResponse{
  }, erro
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
  // Your Code Here (1).
  // Hint: Consider using reader.IterCF
  get, err := server.storage.Reader(req.Context)
  if err != nil{
    return &kvrpcpb.RawScanResponse{
    },err
  }
  limit := req.Limit
  it := get.IterCF(req.Cf)
  defer it.Close()
  var kvs []*kvrpcpb.KvPair
  for it.Seek(req.StartKey);it.Valid();it.Next(){
    item := it.Item()
    v,_ := item.Value()
    k := item.Key()
    res := new(kvrpcpb.KvPair)
    res.Key = k
    res.Value = v
    kvs = append(kvs, res)
    limit = limit -1
    if limit == 0{
      break
    }
  }
  return &kvrpcpb.RawScanResponse{
    Kvs: kvs,
  },nil
}