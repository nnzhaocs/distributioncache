package storage

import (
	"fmt"
	"path"
	"strings"

	//NANNAN
	"io"//	"archive"
//	"crypto/sha512"
//	"io/ioutil"
	"os"
	"path/filepath"
	
	"github.com/docker/distribution"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/pkg/idtools"
	rediscache "github.com/docker/distribution/registry/storage/cache/redis"
	"github.com/garyburd/redigo/redis"
	"github.com/docker/distribution/configuration"
	"github.com/docker/distribution"
	"github.com/opencontainers/go-digest"
)

//NANNAN: dedup operation
//once a layer or manifest is recieved, do dedup:
// read tarfile -> decompress -> unpack -> file digest -> check redis index table ->
// if not -> save, -> update redis index table
// else: -> drop
// update -> redis layer recipe
//
//			==== file level dedup: on disk storage ===
//			1. directory hierarchy, where unique files are saved
//				file_sha256/hex[:1]/sha256digest/filename.extension
//			2. directory,
//				sha256/hex[:1]/sha256digest/data ->
//				sha256/hex[:1]/sha256digest/layer_dirs_hierachy/link_to_uniq_file
//			===== file level dedup: table on redis memory ====
//			added two table:
//					1. index table:
//								 |					|						|
//								 | uniq_file digest	|	location_on_disk	|
//								 |					|						|
//					2. recipe table
//								 |					|								  |
//								 |	layer_digest	|   /path/to/file_name.extension  |
//								 |					|								  |
//			====== file level dedup: cache on redis memory ====
//					1. cache index table:
//					2. see if redis memory can store all recipe table
//								 |					|						|
//								 | uniq_file digest	|	location_on_disk	|
//								 |					|						|
//					3. cache files and cache layers
//
//
//

func DedupLayersFromPath(absPath string) error {
	layerPath := path.Join("/var/lib/registry", absPath)
	
	fmt.Println("NANNAN: START DEDUPLICATION FROM PATH :=>", layerPath)

	parentDir := path.Dir(layerPath)
	unpackPath := path.Join(parentDir, "diff")

	archiver := archive.NewDefaultArchiver()
	options := &archive.TarOptions{
		UIDMaps: archiver.IDMapping.UIDs(),
		GIDMaps: archiver.IDMapping.GIDs(),
	}
	idMapping := idtools.NewIDMappingsFromMaps(options.UIDMaps, options.GIDMaps)
	rootIDs := idMapping.RootPair()
	err := idtools.MkdirAllAndChownNew(unpackPath, 0777, rootIDs)
	if err != nil {
		return err
	}

	err = archiver.UntarPath(layerPath, unpackPath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	
	err = filepath.Walk(unpackPath, checkDuplicate)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

//var files = make(map[[sha512.Size]byte]string)
var dbnoFileDidgest := 1

//const (
//	// Size is the size, in bytes, of a SHA-512 checksum.
//	Size = 64
//
//	// Size224 is the size, in bytes, of a SHA-512/224 checksum.
//	Size224 = 28
//
//	// Size256 is the size, in bytes, of a SHA-512/256 checksum.
//	Size256 = 32
//
//	// Size384 is the size, in bytes, of a SHA-384 checksum.
//	Size384 = 48
//
//	// BlockSize is the block size, in bytes, of the SHA-512/224,
//	// SHA-512/256, SHA-384 and SHA-512 hash functions.
//	BlockSize = 128
//)

digestFn := algorithm.FromReader

func checkDuplicate(path string, info os.FileInfo, err error) error {
	fmt.Printf("NANNAN: START CHECK DUPLICATES :=>")
	if err != nil {
		fmt.Println(err)
		return nil
	}
	
	if info.IsDir() {
		return nil
	}

	fp, err := os.Open(path)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	
	defer fp.Close()

	dgst, err = digestFn(fp)
	if err != nil {
		log.Printf("%s: %v", path, err)
		return nil
	}
	
//	check_from_redis(dgst, dbnoFileDidgest)
	desc, err = statFileDesriptor(dgst, dbnoFileDidgest)
	if err == nil {
		// content already present
		
		//delete this file
		
		//return desc, nil
	} else if err != distribution.ErrBlobUnknown {
		log.Printf("NANNAN: checkDuplicate: error stating content (%v): %v", dgst, err)
		// real error, return it
		fmt.Println(err)
		return nil
		//return distribution.Descriptor{}, err
	}
	
//	var desc distribution.fileDescriptor	
	des := distribution.fileDescriptor{
		
//		Size: int64(len(p)),
		// NOTE(stevvooe): The central blob store firewalls media types from
		// other users. The caller should look this up and override the value
		// for the specific repository.
		filePath: path,
		Digest:    dgst,
	}
	
	err = statFileDesriptor(dgst, des)
	if err != nil {
		return err
	}
	
//	bp, err := bs.path(dgst)
//	if err != nil {
//		return distribution.Descriptor{}, err
//	}
	//log.Warnf("IBM: writing small object %s", mediaType)
	// TODO(stevvooe): Write out mediatype here, as well.
//	return distribution.fileDescriptor{
////		Size: int64(len(p)),
//
//		// NOTE(stevvooe): The central blob store firewalls media types from
//		// other users. The caller should look this up and override the value
//		// for the specific repository.
//		filePath: path,
//		Digest:    dgst,
//	}	
//	if v, ok := files[digest]; ok {
//		fmt.Printf("%q is a duplicate of %q\n", path, v)
//	} else {
//		//check redis file
//		files[digest] = path
//	}

	return nil
}

//"files::sha256:7173b809ca12ec5dee4506cd86be934c4596dd234ee82c0662eac04a8c2c71dc"
func fileDescriptorHashKey(dgst digest.Digest) string {
	return "files::" + dgst.String()
}

//func (rsrbds *repositoryScopedRedisBlobDescriptorService) blobDescriptorHashKey(dgst digest.Digest) string {
//	return "repository::" + rsrbds.repo + "::blobs::" + dgst.String()
//}
//
//func (rsrbds *repositoryScopedRedisBlobDescriptorService) repositoryBlobSetKey(repo string) string {
//	return "repository::" + rsrbds.repo + "::blobs"
//}
//
//type repositoryScopedRedisBlobDescriptorService struct {
//	repo     string
//	upstream *redisBlobDescriptorService
//}

func statFileDesriptor(dgst digest.Digest, dbno int) (distribution.fileDescriptor, error) {
	conn := rediscache.redisPool.Get()
	
	if conn, err = conn.Do("SELECT", dbno); err != nil {
		defer conn.Close()
		return distribution.fileDescriptor{}, err
	}
	
	defer conn.Close()
    
    reply, err := redis.Values(conn.Do("HMGET", fileDescriptorHashKey(dgst), "digest", "filePath")//, "fileSize", "layerDescriptor"))
	if err != nil {
		return distribution.fileDescriptor{}, err
	}

	// NOTE(stevvooe): The "size" field used to be "length". We treat a
	// missing "size" field here as an unknown blob, which causes a cache
	// miss, effectively migrating the field.
	if len(reply) < 2 || reply[0] == nil || reply[1] == nil { // don't care if mediatype is nil
		return distribution.fileDescriptor{}, distribution.ErrBlobUnknown
	}

	var desc distribution.fileDescriptor
	if _, err := redis.Scan(reply, &desc.Digest, &desc.filePath); err != nil {
		return distribution.fileDescriptor{}, err
	}

	return desc, nil
}

func (rbds *redisBlobDescriptorService) setFileDescriptor(dgst digest.Digest, desc distribution.fileDescriptor) error {
	
	conn := rediscache.redisPool.Get()
	
	if conn, err = conn.Do("SELECT", dbno); err != nil {
		defer conn.Close()
		return err
	}
	
	defer conn.Close()
	
	if _, err := conn.Do("HMSET", fileDescriptorHashKey(dgst),
		"digest", desc.Digest,
		"filePath", desc.filePath); err != nil {
		return err
	}

//	// Only set mediatype if not already set.
//	if _, err := conn.Do("HSETNX", rbds.blobDescriptorHashKey(dgst),
//		"mediatype", desc.MediaType); err != nil {
//		return err
//	}

	return nil
}

// //a pool embedding the original pool and adding adbno state
//type DedupRedisPool struct {
//   Pool redis.Pool
//   dbno int
//}
// "overriding" the Get method
//func (drp *DedupRedisPool)Get() Connection {
//   conn := drp.Pool.Get()
//   conn.Do("SELECT", drp.dbno)
//   return conn
//}

//var fileDigestPool DedupRedisPool

//var background = &instanceContext{
//	Context: context.Background(),
//}
//fileDigestPool := &DedupRedisPool {
//    redis.Pool{
//        MaxIdle:   80,
//        MaxActive: 12000, // max number of connections
//        Dial: func() (redis.Conn, error) {
//        c, err := redis.Dial("tcp", host+":"+port)
//        if err != nil {
//            panic(err.Error())
//        }
//        return c, err
//    },
//    3, // the db number
//}
//    //now you call it normally
//conn := fileDigestPool.Get()
//defer conn.Close()

//func applyDiff(layerPath string, diff io.Reader) error {
//	//path := path.Join(layerPath, "diff")
//	fmt.Println("NANNAN: start unpacking layer")
//	parentDir := path.Dir(layerPath)
//	diffPath := path.Join(parentDir, "diff")
//
//	uidMap := []idtools.IDMap{
//		{
//			ContainerID: 0,
//			HostID:      os.Getuid(),
//			Size:        1,
//		},
//	}
//	gidMap := []idtools.IDMap{
//		{
//			ContainerID: 0,
//			HostID:      os.Getgid(),
//			Size:        1,
//		},
//	}
//	//	options := graphdriver.Options{Root: td, UIDMaps: uidMap, GIDMaps: gidMap}
//	return chrootarchive.UntarUncompressed(diff, diffPath, &archive.TarOptions{
//		UIDMaps: uidMap,
//		GIDMaps: gidMap,
//	})
}

//	uidMaps       []idtools.IDMap
//	gidMaps       []idtools.IDMap

// ApplyDiff extracts the changeset from the given diff into the
// layer with the specified id and parent, returning the size of the
// new layer in bytes.
//func ApplyDiff(id, parent string, diff io.Reader) (size int64, err error) {
//	if !a.isParent(id, parent) {
//		return a.naiveDiff.ApplyDiff(id, parent, diff)
//	}
//
//	// AUFS doesn't need the parent id to apply the diff if it is the direct parent.
//	if err = a.applyDiff(id, diff); err != nil {
//		return
//	}
//
//	return a.DiffSize(id, parent)
//}

//func (ls *layerStore) applyTar(tx *fileMetadataTransaction, ts io.Reader, parent string, layer *roLayer) error {
//	digester := digest.Canonical.Digester()
//	tr := io.TeeReader(ts, digester.Hash())
//
//	rdr := tr
//	if ls.useTarSplit {
//		tsw, err := tx.TarSplitWriter(true)
//		if err != nil {
//			return err
//		}
//		metaPacker := storage.NewJSONPacker(tsw)
//		defer tsw.Close()
//
//		// we're passing nil here for the file putter, because the ApplyDiff will
//		// handle the extraction of the archive
//		rdr, err = asm.NewInputTarStream(tr, metaPacker, nil)
//		if err != nil {
//			return err
//		}
//	}
//
//	applySize, err := ls.driver.ApplyDiff(layer.cacheID, parent, rdr)
//	if err != nil {
//		return err
//	}
//
//	// Discard trailing data but ensure metadata is picked up to reconstruct stream
//	io.Copy(ioutil.Discard, rdr) // ignore error as reader may be closed
//
//	layer.size = applySize
//	layer.diffID = DiffID(digester.Digest())
//
//	logrus.Debugf("Applied tar %s to %s, size: %d", layer.diffID, layer.cacheID, applySize)
//
//	return nil
//}

//// Diff produces an archive of the changes between the specified
//// layer and its parent layer which may be "".
//func Diff(id, parent string) (io.ReadCloser, error) {
////	if !a.isParent(id, parent) {
////		return a.naiveDiff.Diff(id, parent)
////	}
//	// AUFS doesn't need the parent layer to produce a diff.
//	return archive.TarWithOptions(path.Join(a.rootPath(), "diff", id), &archive.TarOptions{
//		Compression:     archive.Uncompressed,
//		ExcludePatterns: []string{archive.WhiteoutMetaPrefix + "*", "!" + archive.WhiteoutOpaqueDir},
//		UIDMaps:         a.uidMaps,
//		GIDMaps:         a.gidMaps,
//	})
//}
