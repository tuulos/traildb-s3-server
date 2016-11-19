package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/augmentedtree"
	"github.com/bluele/gcache"
)

type BlockResponse struct {
	path   string
	offset uint64
	size   uint64
}

type S3Loader func(string, string, string, uint64, uint64) (uint64, error)

const MAX_RETRIES = 5

/*
This file takes care of three operations through three core data
structures which are listed below:

1. Map a new range request to previously cached ranges (blocks)
   using an augmentedtree, RangeIndex. The ranges are specific to
   each tdb so we need a separate mapping, rangeIndices, that maps
   a tdb url to a RangeIndex.
*/
type RangeIndex struct {
	index augmentedtree.Tree
	lock  sync.Mutex
}

var rangeIndicesLock sync.Mutex
var rangeIndices map[string]*RangeIndex

/*
2. Keep track of usage of blocks so we can evict unused blocks.
   This is accompished using an ARC cache blockCache that tracks
   both recency and frequency of usage.
*/
var blockCache gcache.Cache

/*
3. When a block is missing from the cache, we need to fetch it
   from S3. It is quite possible that multiple clients need
   the same block, e.g. a new tdb, exactly at the same time but
   it would be wasteful to download the same block from S3 multiple
   times. We control this with loaderState that allows only one
   loader to download a particular block while forcing others to
   wait.
*/
var loaderStateLock sync.Mutex
var loaderState map[string]*sync.WaitGroup

var blockSize uint64

/*
The block is missing. Fetch it from S3 while controlling the
number of simultaneous loaders for this block.
*/
func loadBlock(reqBlock *ExtBlock, loader S3Loader) error {
	key := reqBlock.GetKey()
	loaderStateLock.Lock()
	existingWG, ok := loaderState[key]
	if ok {
		/* an existing loader active, wait */
		loaderStateLock.Unlock()
		existingWG.Wait()
	} else {
		/* this is the first loader, start loading, block others */
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
			/* block received successfully, add it to blockCache */
			rangeIndicesLock.Lock()
			rangeIndex, ok := rangeIndices[reqBlock.GetPath()]
			rangeIndicesLock.Unlock()
			/* !ok = the key got evicted meanwhile */
			if ok {
				rangeIndex.lock.Lock()
				reqBlock.SetRealSize(realSize)
				rangeIndex.lock.Unlock()
				fmt.Printf("%s added to block cache\n", key)
				blockCache.Set(key, reqBlock)
			}
		}
		loaderStateLock.Lock()
		delete(loaderState, key)
		loaderStateLock.Unlock()
		newWG.Done()
	}
	return nil
}

/*
main function to request block from the cache or S3
*/
func getBlockFromCache(
	root string,
	url string,
	offset uint64,
	minSize uint64,
	loader S3Loader) (BlockResponse, error) {

	/*
	   create a new ExtBlock for this request. This may get replaced
	   by another cached block if we find a match..
	*/
	reqSize := blockSize
	if minSize > blockSize {
		reqSize = minSize
	}
	reqBlock := NewExtBlock(root, url, offset, reqSize)

	for i := 0; i < MAX_RETRIES; i++ {

		/*
		   find existing blocks in the range cache for this url which may
		   include this block as a subrange
		*/
		rangeIndicesLock.Lock()
		rangeIndex, ok := rangeIndices[url]
		if !ok {
			rangeIndex = &RangeIndex{index: augmentedtree.New(1)}
			rangeIndices[url] = rangeIndex
		}
		rangeIndex.lock.Lock()
		rangeIndicesLock.Unlock()

		var match augmentedtree.Interval = nil
		for _, block := range rangeIndex.index.Query(reqBlock) {
			/*
			   a matching block must include the full range of the requested
			   block
			*/
			if offset >= uint64(block.LowAtDimension(1)) &&
				uint64(block.HighAtDimension(1))-offset >= minSize-1 {
				match = block
				break
			}
		}
		if match == nil {
			/* no matching block found in the cache, use the loader */
			fmt.Printf("Adding a new entry in range cache for %s offset %d\n", url, offset)
			rangeIndex.index.Add(reqBlock)
			rangeIndex.lock.Unlock()
			match = reqBlock
		} else {
			/*
			   a matching range found in the range cache. Next we need to check
			   that it actually exists in blockCache (it might not exists, e.g. due
			   to being loaded right now.
			*/
			rangeIndex.lock.Unlock()
			key := (match.(*ExtBlock)).GetKey()
			fmt.Printf("Found a match for %s (%d) -> %s (%d)\n", reqBlock.GetKey(), offset, key, (match.(*ExtBlock)).GetOffset())
			val, err := blockCache.GetIFPresent(key)
			if err == nil {
				fmt.Printf("Cache hit for %s\n", key)
				/* the block found in the cache, return the result */
				res := val.(*ExtBlock)
				path := path.Join(res.GetDir(), res.GetKey())
				respOffset := offset - uint64(res.LowAtDimension(1))
				respSize := res.GetRealSize() - respOffset
				return BlockResponse{path: path,
					offset: respOffset,
					size:   respSize}, nil
			}
		}
		/* no previously cached block found, use the loader */
		err := loadBlock(match.(*ExtBlock), loader)
		if err != nil {
			s3err, ok := err.(*s3error)
			if ok && s3err.notFound {
				return BlockResponse{}, err
			} else {
				/*
				   loader failed, hopefully a temporary issue, let's try again
				*/
				fmt.Printf("[warn] Load block failed: %s", err.Error())
				time.Sleep(10 * time.Second)
			}
		}
	}
	/*
	   all attempts failed to download the block.
	   The client may try again later */
	return BlockResponse{},
		errors.New(fmt.Sprintf("GetBlock failed for %s (offset %d size %d)",
			url, offset, minSize))
}

func evict(keyIn interface{}, valueIn interface{}) {
	key := keyIn.(string)
	val, ok := valueIn.(*ExtBlock)
	if ok {
		rangeIndicesLock.Lock()
		rangeIndex, ok := rangeIndices[val.GetPath()]
		rangeIndicesLock.Unlock()
		if ok {
			rangeIndex.lock.Lock()
			rangeIndex.index.Delete(val)
			rangeIndex.lock.Unlock()
		}
		del := path.Join(val.GetDir(), val.GetKey())
		err := os.Remove(del)
		if err != nil {
			fmt.Printf("deleting %s failed: %s\n", del, err.Error())
		}
	} else {
		_, val := valueIn.(string)
		fmt.Printf("Evict strangeness %s %s \n", key, val)
	}
}

/* initialize the three caches */
func initCache(maxSize uint64, blockSize0 uint64) {
	blockSize = blockSize0
	rangeIndices = make(map[string]*RangeIndex)
	loaderState = make(map[string]*sync.WaitGroup)
	numBlocks := int((maxSize * 1024 * 1024) / BLOCKSIZE)
	/* TODO pre-populate cache with entries from disk */
	blockCache = gcache.New(numBlocks).ARC().EvictedFunc(evict).Build()
}
