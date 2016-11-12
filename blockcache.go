package main

import (
    "sync"
    "time"
    "fmt"
    "errors"

    "github.com/bluele/gcache"
    "github.com/Workiva/go-datastructures/augmentedtree"
)
/*
# State

- Interval Cache R
- Expiration Cache E
- Loader State L

# Read Request
- check interval cache R for an existing block
    - found B
        - get it from expiration cache E
            - found:
                - return the path
            - not found:
                - loader B
    - not found
        - define a new block B
        - add B to R
        - loader B

# Loader, invoked if the key doesn't exist in E

- incoming request for a block B
- lock L
- get B form L
  - found:
        - got WaitGroup G
        - unlock L
        - G.Wait()
  - not found:
        - put WaitGroup B->G in L
        - G.Add(1)
        - unlock L
        - load B from S3
        - put B in E
        - lock L
        - remove B->G from L
        - unlock L
        - G.done()
- go to the begining of READ path

#  Eviction function, invoked by E if E full

- remove B from R
- unlink B file
- remove B from E

*/

type BlockResponse struct {
    path string
    offset uint64
    size uint64
}

type RangeIndex struct {
    index augmentedtree.Tree
    lock sync.Mutex
}

type S3Loader func(string, string, string, uint64, uint64) (uint64, error)

const MAX_RETRIES = 5

var rangeIndicesLock sync.Mutex
var rangeIndices map[string] RangeIndex

var blockCache gcache.Cache

var loaderStateLock sync.Mutex
var loaderState map[string] *sync.WaitGroup

var blockSize uint64

func loadBlock(reqBlock *ExtBlock, loader S3Loader) error {
    key := reqBlock.GetKey()
    loaderStateLock.Lock()
    existingWG, ok := loaderState[key]
    if ok {
        loaderStateLock.Unlock()
        existingWG.Wait()
    } else {
        var newWG sync.WaitGroup
        newWG.Add(1)
        loaderState[key] = &newWG
        loaderStateLock.Unlock()
        realSize, err := loader(reqBlock.GetDir(),
                                reqBlock.GetKey(),
                                reqBlock.GetPath(),
                                reqBlock.GetOffset(),
                                reqBlock.GetSize())
        if err != nil {
            fmt.Printf("loader failed: %s\n", err.Error())
            return err
        } else {
            reqBlock.SetRealSize(realSize)
            fmt.Printf("%s added to block cache\n", key)
            blockCache.Set(key, reqBlock)
        }
        loaderStateLock.Lock()
        delete(loaderState, key)
        loaderStateLock.Unlock()
        newWG.Done()
    }
    return nil
}

func getBlockFromCache(url string,
                       offset uint64,
                       minSize uint64,
                       loader S3Loader) (BlockResponse, error) {

    reqSize := blockSize
    if minSize > blockSize {
        reqSize = minSize
    }
    reqBlock := NewExtBlock(url, offset, reqSize)

    for i := 0; i < MAX_RETRIES; i++ {

        rangeIndicesLock.Lock()
        rangeIndex, ok := rangeIndices[url]
        if !ok {
            rangeIndex = RangeIndex{index: augmentedtree.New(1)}
            rangeIndices[url] = rangeIndex
        }
        rangeIndex.lock.Lock()
        rangeIndicesLock.Unlock()

        var match augmentedtree.Interval = nil
        for _, block := range rangeIndex.index.Query(reqBlock) {
            if offset >= uint64(block.LowAtDimension(1)) &&
               uint64(block.HighAtDimension(1)) - offset >= minSize - 1 {
                match = block
                break
            }
        }
        if match == nil {
            fmt.Printf("Adding a new entry in range cache for %s offset %d\n", url, offset)
            rangeIndex.index.Add(reqBlock)
            rangeIndex.lock.Unlock()
            match = reqBlock
        }else{
            rangeIndex.lock.Unlock()
            key := (match.(*ExtBlock)).GetKey()
            fmt.Printf("Found a match for %s (%d) -> %s (%d)\n", reqBlock.GetKey(), offset, key, (match.(*ExtBlock)).GetOffset())
            val, err := blockCache.GetIFPresent(key)
            if err == nil {
                res := val.(*ExtBlock)
                path := fmt.Sprintf("%s/%s", res.GetDir(), res.GetKey())
                respOffset := offset - uint64(res.LowAtDimension(1))
                respSize := res.GetRealSize() - respOffset
                return BlockResponse{path: path,
                                     offset: respOffset,
                                     size: respSize}, nil
            }
        }
        err := loadBlock(match.(*ExtBlock), loader)
        if err != nil {
            fmt.Printf("[warn] Load block failed: %s", err.Error())
            time.Sleep(10 * time.Second)
        }
    }
    return BlockResponse{},
           errors.New(fmt.Sprintf("GetBlock failed for %s (offset %d size %d)",
                                  url, offset, minSize))
}

func initCache(maxSize uint64, blockSize0 uint64) {
    blockSize = blockSize0
    rangeIndices = make(map[string] RangeIndex)
    loaderState = make(map[string] *sync.WaitGroup)
    numBlocks := int((maxSize * 1024 * 1024) / BLOCKSIZE)
    /* NOTE add evictionfunction */
    blockCache = gcache.New(numBlocks).ARC().Build()
}
