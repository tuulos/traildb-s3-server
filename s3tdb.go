package main

import (
    "fmt"
    "net"
    "os"
    "path"
    "flag"
    "errors"
    "encoding/binary"
    "strings"
    "net/url"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

type extPacket struct {
    packetType string
    offset uint64
    size uint64
    path string
}

type s3error struct {
    notFound bool
    message string
}

const TDB_EXT_MAX_PATH_LEN = 1024
const TDB_EXT_PACKET_HEAD_SIZE = 24
const BLOCKSIZE = 16 * 1024 * 1024

var conf_root string

func s3get(s3conn *s3.S3, path string, offset uint64, size uint64) (*s3.GetObjectOutput, *s3error) {
    s3url, err := url.Parse(path)
    if err != nil {
        return nil, &s3error{notFound: true, message: "parsing failed"}
    }
    fmt.Printf("Bucket %s KEY %s offset %d size %d\n", s3url.Host, s3url.Path, offset, size)
    rrange := fmt.Sprintf("bytes=%d-%d", offset, offset + size - 1)
    resp, err := s3conn.GetObject(&s3.GetObjectInput{
        Range: &rrange,
        Bucket: &s3url.Host,
        Key: aws.String(strings.TrimLeft(s3url.Path, "/")),
    })
    if err != nil {
        fmt.Printf("S3 failure %s \n", err)
        notFound := false
        if (err.(awserr.Error)).Code() == "NoSuchKey" {
            notFound = true
        }
        return nil, &s3error{notFound: notFound, message: err.Error()}
    }
    return resp, nil
}

func s3loader(s3conn *s3.S3,
              buf []byte,
              dir string,
              fname string,
              url string,
              offset uint64,
              maxSize uint64) (uint64, error) {


    root := path.Join(conf_root, dir)
    err := os.MkdirAll(root, 0755)
    if err != nil {
        err = errors.New(fmt.Sprintf("Could not create directory %s: %s",
                         root,
                         err.Error()))
        return 0, err
    }
    dstPath := path.Join(root, fname)
    fmt.Printf("Now downloading %s (offs %d) to %s\n", url, offset, dstPath)
    dst, err := os.Create(dstPath)
    if err != nil {
        err = errors.New(fmt.Sprintf("Could not create a block file %s: %s",
                                     dstPath,
                                     err.Error()))
        return 0, err
    }
    resp, s3err := s3get(s3conn, url, offset, maxSize)
    if s3err != nil {
        err = errors.New(fmt.Sprintf("S3 failure: %s", s3err.message))
        return 0, err
    }
    for total := uint64(0); total < uint64(*resp.ContentLength); {
        n, err := resp.Body.Read(buf)
        if n == 0 && err != nil {
            return 0, err
        }
        _, err = dst.Write(buf[:n])
        if err != nil {
            err = errors.New(fmt.Sprintf("Writing to %s failed: %s",
                             dstPath,
                             err.Error()))
            return 0, err
        }
        total += uint64(n)
    }
    err = dst.Close()
    if err != nil {
        err = errors.New(fmt.Sprintf("Closing %s failed: %s",
                         dstPath,
                         err.Error()))
        return 0, err
    }
    fmt.Printf("Download %s successful. Real size is %d\n", dstPath, *resp.ContentLength)
    return uint64(*resp.ContentLength), nil
}

func handshake(req extPacket, s3conn *s3.S3) extPacket {
    if req.path[:5] != "s3://" {
        return extPacket{packetType: "PROT"}
    }
    _, err := s3get(s3conn, req.path, 0, 1)
    if err == nil {
        return extPacket{packetType: "OKOK"}
    } else if err.notFound {
        return extPacket{packetType: "MISS"}
    } else {
        return extPacket{packetType: "FAIL"}
    }
}

func readBytes(conn net.Conn, buf []byte) error {
    for n := 0; n < len(buf); {
        r, err := conn.Read(buf[n:])
        if err != nil || r == 0 {
            return errors.New("conn.Read failed")
        }
        n += r
    }
    return nil
}

func readRequest(conn net.Conn, buf []byte) (extPacket, error) {
    req := extPacket{}
    err := readBytes(conn, buf[:TDB_EXT_PACKET_HEAD_SIZE])
    if err != nil {
        return req, err
    }
    req.packetType = string(buf[:4])
    req.offset = binary.LittleEndian.Uint64(buf[4:12])
    req.size = binary.LittleEndian.Uint64(buf[12:20])
    pathLen := binary.LittleEndian.Uint32(buf[20:24])
    err = readBytes(conn, buf[:pathLen])
    if err != nil {
        return req, err
    }
    req.path = string(buf[:pathLen])
    fmt.Printf("%s Offset %d size %d path %s\n", req.packetType, req.offset, req.size, req.path)
    return req, nil
}

func sendBytes(conn net.Conn, buf []byte) error {
    for n := 0; n < len(buf); {
        r, err := conn.Write(buf[n:])
        if err != nil || r == 0 {
            return errors.New("conn.Send failed")
        }
        n += r
    }
    return nil
}

func sendResponse(conn net.Conn, buf []byte, resp extPacket) error {
    fmt.Printf("Send response %s %d %d %s\n", resp.packetType, resp.offset, resp.size, resp.path)
    copy(buf[:4], []byte(resp.packetType))
    binary.LittleEndian.PutUint64(buf[4:12], resp.offset)
    binary.LittleEndian.PutUint64(buf[12:20], resp.size)
    binary.LittleEndian.PutUint32(buf[20:24], uint32(len(resp.path)))
    copy(buf[24:], []byte(resp.path))
    return sendBytes(conn, buf[:24 + len(resp.path)])
}

func getCachedBlock(req extPacket, s3conn *s3.S3, buffer []byte) extPacket {
    var loader =
        func (dir string,
              fname string,
              url string,
              offset uint64,
              maxSize uint64) (uint64, error) {
            return s3loader(s3conn, buffer, dir, fname, url, offset, maxSize)
        }
    resp, err := getBlockFromCache(req.path, req.offset, req.size, loader)
    if err != nil {
        return extPacket{packetType: "FAIL"}
    } else {
        return extPacket{packetType: "OKOK",
                         offset: resp.offset,
                         size: resp.size,
                         path: path.Join(conf_root, resp.path)}
    }
}


func handleConnection(conn net.Conn, region string) {
    fmt.Println("New connection!")

    svc := session.New(&aws.Config{Region: aws.String(region)})
    s3conn := s3.New(svc)

    buf := make([]byte, TDB_EXT_PACKET_HEAD_SIZE + TDB_EXT_MAX_PATH_LEN)
    for {
        req, err := readRequest(conn, buf)
        if err == nil {
            if req.packetType == "V000" {
                err = sendResponse(conn, buf, handshake(req, s3conn))
            } else if req.packetType == "READ" {
                err = sendResponse(conn, buf, getCachedBlock(req, s3conn, buf))
            } else if req.packetType == "EXIT" {
                err = errors.New("exit")
            }
        }
        if err != nil {
            conn.Close()
            return
        }
    }
}

func serve(port int, region string) {
    host := fmt.Sprintf("localhost:%d", port)
    serv, err := net.Listen("tcp", host)
    if err != nil {
        fmt.Println(err.Error())
        os.Exit(1)
    }
    for {
        conn, err := serv.Accept()
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
        }
        go handleConnection(conn, region)
    }
}

func main() {
    root := flag.String("root", ".", "where to store blocks")
    maxSize := flag.Int("max-size", 1000, "how many MBs to use max")
    port := flag.Int("port", 9009, "port to listen to")
    region := flag.String("region", "us-west-2", "s3 region")
    flag.Parse()

    cwd, err := os.Getwd()
    if err != nil {
        fmt.Printf("Couldn't get the current working directory: %s\n", err.Error)
        os.Exit(1)
    }

    conf_root = path.Clean(path.Join(cwd, *root))
    fmt.Printf("\ntraildb-s3-server\n\nStarting the server at localhost:%d.\nAt most %dMB of blocks will be cached at %s\n",
               *port, *maxSize, conf_root)

    initCache(uint64(*maxSize), BLOCKSIZE)
    serve(*port, *region)
}
