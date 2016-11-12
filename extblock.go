package main

import (
    "fmt"
    "strconv"
    "strings"
    "crypto/md5"
    "encoding/base64"
    "encoding/binary"
    "encoding/hex"

    "github.com/Workiva/go-datastructures/augmentedtree"
)

type ExtBlock struct {
    offset      uint64
    blockSize   uint64
    realSize    uint64
    key         string
    dir         string
    path        string
    hash        [16]byte
}

func (e ExtBlock) LowAtDimension(dim uint64) int64 {
    return int64(e.offset)
}

func (e ExtBlock) HighAtDimension(dim uint64) int64 {
    return int64(e.offset + e.blockSize - 1)
}

func (e ExtBlock) OverlapsAtDimension(other augmentedtree.Interval, dim uint64) bool {
    return (other.LowAtDimension(dim) >= e.LowAtDimension(dim) &&
            other.LowAtDimension(dim) <= e.HighAtDimension(dim))
}

func (e ExtBlock) ID() uint64 {
    return binary.LittleEndian.Uint64(e.hash[:8])
}

func (e *ExtBlock) GetKey() string {
    return e.key
}

func (e *ExtBlock) GetSize() uint64 {
    return e.blockSize
}

func (e *ExtBlock) GetPath() string {
    return e.path
}

func (e *ExtBlock) GetOffset() uint64 {
    return e.offset
}

func (e *ExtBlock) GetDir() string {
    return hex.EncodeToString(e.hash[:1])
}

func (e *ExtBlock) SetRealSize(size uint64) {
    e.realSize = size
}

func (e *ExtBlock) GetRealSize() uint64 {
    return e.realSize
}

func NewExtBlock(path string, offset uint64, size uint64) *ExtBlock {
    keyStr := fmt.Sprintf("%s %d %d", path, offset, size)
    key := base64.URLEncoding.EncodeToString([]byte(keyStr))
    return &ExtBlock{key: key,
                     offset: offset,
                     blockSize: size,
                     path: path,
                     hash: md5.Sum([]byte(key))}
}

func ParseExtBlock(key string, realSize uint64) (*ExtBlock, error) {
    decoded, err := base64.URLEncoding.DecodeString(key)
    if err != nil {
        return nil, err
    }
    parts := strings.Split(string(decoded), " ")
    offset, err := strconv.ParseUint(parts[1], 10, 64)
    if err != nil {
        return nil, err
    }
    blockSize, err := strconv.ParseUint(parts[2], 10, 64)
    if err != nil {
        return nil, err
    }
    return &ExtBlock{key: key,
                     offset: offset,
                     blockSize: blockSize,
                     realSize: realSize,
                     path: parts[0],
                     hash: md5.Sum([]byte(key))}, nil
}




